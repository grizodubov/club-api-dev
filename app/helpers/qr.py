import os
import orjson
import segno



################################################################
def create_code(type, file, data):
    path = '/var/www/media.clubgermes.ru/html/qr/' + type + '/' + file + '.png'
    str = orjson.dumps(data)
    if not os.path.exists(path):
        qrcode = segno.make_qr(str)
        qrcode.save(path , scale = 25)
    return 'https://media.clubgermes.ru/qr/' + type + '/' + file + '.png'
