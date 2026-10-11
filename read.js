const pcsclite = require('@pokusew/pcsclite');
const iconv = require('iconv-lite');
const WebSocket = require('ws');

// 1. เปิด WebSocket Server ที่ Port 8080
const wss = new WebSocket.Server({ port: 8080 });

wss.on('connection', (ws) => {
    console.log('🌐 [WebSocket] มี Client เชื่อมต่อเข้ามาใหม่');
    ws.send(JSON.stringify({ status: 'connected', message: 'รอการเสียบบัตร...' }));
});

console.log('========================================');
console.log(' ระบบอ่านบัตร + WebSocket Server');
console.log(' รันเซิร์ฟเวอร์ที่ ws://localhost:8080');
console.log('========================================');

// 2. ระบบอ่านบัตร PCSC
const pcsc = pcsclite();

const transmit = (reader, protocol, data) => {
    return new Promise((resolve, reject) => {
        reader.transmit(data, 256, protocol, (err, res) => {
            if (err) reject(err);
            else resolve(res);
        });
    });
};

const readBlock = async (reader, protocol, offsetHigh, offsetLow, length) => {
    const cmd = Buffer.from([0x80, 0xB0, offsetHigh, offsetLow, 0x02, 0x00, length]);
    await transmit(reader, protocol, cmd);
    const respCmd = Buffer.from([0x00, 0xC0, 0x00, 0x00, length]);
    const data = await transmit(reader, protocol, respCmd);
    return data.slice(0, length);
};

const cleanStr = (buffer) => {
    return iconv.decode(buffer, 'tis620').replace(/#/g, ' ').replace(/\s+/g, ' ').trim();
};

pcsc.on('reader', function(reader) {
    reader.on('status', function(status) {
        const changes = this.state ^ status.state;
        if (changes) {
            if ((status.state & this.SCARD_STATE_PRESENT) && !(this.state & this.SCARD_STATE_PRESENT)) {
                console.log('💳 ตรวจพบบัตรประชาชน กำลังดึงข้อมูล...');

                reader.connect({ share_mode: this.SCARD_SHARE_SHARED }, async function(err, protocol) {
                    if (err) return console.error('เชื่อมต่อบัตรผิดพลาด:', err);

                    try {
                        const SELECT_APDU = Buffer.from([0x00, 0xA4, 0x04, 0x00, 0x08, 0xA0, 0x00, 0x00, 0x00, 0x54, 0x48, 0x00, 0x01]);
                        await transmit(reader, protocol, SELECT_APDU);

                        const rawCid = await readBlock(reader, protocol, 0x00, 0x04, 13);
                        const rawNameTh = await readBlock(reader, protocol, 0x00, 0x11, 100);
                        const rawNameEn = await readBlock(reader, protocol, 0x00, 0x75, 100);
                        const rawDob = await readBlock(reader, protocol, 0x00, 0xD9, 8);
                        const rawAddress = await readBlock(reader, protocol, 0x15, 0x79, 100);

                        const cid = rawCid.toString('ascii');
                        const nameTh = cleanStr(rawNameTh);
                        const nameEn = cleanStr(rawNameEn);
                        const address = cleanStr(rawAddress);

                        const dobStr = rawDob.toString('ascii');
                        const dob = `${dobStr.substring(6,8)}/${dobStr.substring(4,6)}/${dobStr.substring(0,4)}`;

                        const cardData = {
                            status: 'success',
                            data: { cid, nameTh, nameEn, dob, address }
                        };

                        console.log(`✅ อ่านข้อมูลสำเร็จ: ${nameTh}`);

                        // ส่งข้อมูลออกไปให้ WebSocket Client (หน้าเว็บ)
                        wss.clients.forEach(client => {
                            if (client.readyState === WebSocket.OPEN) {
                                client.send(JSON.stringify(cardData));
                            }
                        });

                    } catch (error) {
                        console.error('❌ เกิดข้อผิดพลาดในการดึงข้อมูล:', error);
                    } finally {
                        reader.disconnect(reader.SCARD_LEAVE_CARD, (err) => {
                            if (err) console.error('ปิดการเชื่อมต่อผิดพลาด:', err);
                        });
                    }
                });
            }
        }
    });
});