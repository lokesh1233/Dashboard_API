import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from config.env import env
import datetime

class generateEmail():

    def __init__(self):
        pass


    def SendMail(self):

        sender_address = env.senderEmail
        sender_pass = env.senderPassword
        receiver_address=""

        message = MIMEMultipart()
        message['From'] = sender_address
        to = receiver_address
        # receiver = to.split(",")
        receiver=to
        message['To'] = to
        today = datetime.datetime.today().date().strftime('%d %B %Y')

        mail_content = str('''
    Hi,

    Please make a note that the user limit has exceeded.
    Kindly reach out to us for more queries.

    Regards

                ''').format(today)
        message['Subject'] = 'Chatbot user limit exceeded warning'

        print(receiver)
        message.attach(MIMEText(mail_content, 'plain'))

        # payload = MIMEBase('application', 'octet-stream')

        session = smtplib.SMTP('smtp.gmail.com', 587)
        session.starttls()
        session.login(sender_address, sender_pass)
        text = message.as_string()
        session.sendmail(sender_address, receiver, text)
        session.quit()
        return "Mail sent"



