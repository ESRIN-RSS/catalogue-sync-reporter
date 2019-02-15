import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os.path import basename


def send_from_gmail(send_to, subject, text, html=None, files=None):
    """Send email from RSS gmail account

    :param send_to: list of strings containing email addresses
    :param subject: subject of the email
    :param text: body contents of email
    :param html: contents of the body in HTML format
    :param files: list of files to attach
    :return:
    """
    assert isinstance(send_to, list)

    if html:
        msg = MIMEMultipart('alternative')
        msg.attach(MIMEText(text, 'plain'))
        msg.attach(MIMEText(html, 'html'))
    else:
        msg = MIMEMultipart()
        msg.attach(MIMEText(text))
    msg['From'] = 'esa.rss.team@gmail.com'
    msg['Subject'] = subject

    for f in files or []:
        with open(f, "rb") as fil:
            msg.attach(MIMEApplication(
                fil.read(),
                Content_Disposition='attachment; filename="%s"' % basename(f),
                Name=basename(f)
            ))

    smtp = smtplib.SMTP_SSL('smtp.gmail.com')
    smtp.ehlo()
    smtp.login('esa.rss.team', 'ioehroher34yc844nywrckd')
    smtp.sendmail('esa.rss.team@gmail.com', send_to, msg.as_string())
    smtp.close()
