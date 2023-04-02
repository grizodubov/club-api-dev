import re
from email.mime.text import MIMEText
from email.header import Header
import aiosmtplib
from jinja2 import Template



################################################################
def send_email(stream, email, subject, body, data = {}):
    subject_template = Template(subject)
    body_template = Template(body)
    stream.register(
        send,
        email = email,
        subject = subject_template.render(data),
        body = body_template.render(data),
    )



################################################################
async def send(email, subject, body):
    print('SENDING EMAIL:', email, subject)
    # print(body)
    SMTP_SERVER = "smtp.yandex.ru"
    SMTP_PORT = 587
    SENDER_EMAIL = "info@digitender.ru"
    SENDER_PASSWORD = "nocoiavacbfkslgf"
    if email and re.match(r"[^@]+@[^@]+\.[^@]+", email):
        message = MIMEText(body, 'html', 'utf-8')
        message['Subject'] = Header(subject, 'utf-8')
        message['From'] = SENDER_EMAIL
        message['To'] = email    
        server = aiosmtplib.SMTP(hostname = SMTP_SERVER, port = SMTP_PORT)
        await server.connect()
        await server.ehlo()
        # await server.starttls()
        await server.login(SENDER_EMAIL, SENDER_PASSWORD)
        try:
            await server.sendmail(SENDER_EMAIL, email, message.as_string())
        except Exception as e:
            print('MAIL: SEND ERROR', e)
            raise Exception('Mail send error')
        print('MAIL SENT!')
    else:
        print('MAIL: WRONG EMAIL', email)
