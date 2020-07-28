import json
import pendulum
import smtplib
import schedule
import pymongo
import base64
import email.message
from jinja2 import Template


with open("config.json", encoding="utf-8") as json_config:
    config = json.load(json_config)


def get_records(cfg):
    """
    Connecting to the database and getting new records in 24 hours

    :param cfg: (dict) data from config.json
    :return: (pymongo.Cursor) or (None)
    """

    client = pymongo.MongoClient(cfg['mongo']['connection'])
    db = client[cfg['mongo']['database_name']]
    collection = db[cfg['mongo']['collection_name']]

    dt = pendulum.now()
    dt = dt.subtract(days=1)

    filter_ = {"time_enrolled": {"$gt": dt}}
    fields_ = {"_id": 1, "uid": 1, "name": 1, "time_enrolled": 1, "photo": 1}
    collection.create_index("time_enrolled")
    cursor = collection.find(filter_, fields_).sort("time_enrolled", pymongo.ASCENDING)

    if collection.count_documents(filter_) == 0:
        cursor = None

    client.close()
    return cursor


def send_email(cursor, cfg):
    """
    Data from the cursor is inserted into an html table and sent to the email address

    :param cursor: (pymongo.Cursor) new records for 24 hours,
                    if it is equal to (None) then there are no new records
    :param cfg: (dict) data from config.json
    """

    msg = email.message.Message()
    msg['Subject'] = cfg['other']['email_header']
    password = cfg['email']['password']
    msg['From'] = cfg['email']['from']
    msg['To'] = cfg['email']['to']

    b64 = {}
    if cursor is not None:
        for i in cursor:
            try:
                path = f"{cfg['other']['path_to_photos']}/{i['uid']}/photo_{i['uid']}.png"
                image_file = open(path, 'rb')
                b64[i['uid']] = base64.b64encode(image_file.read()).decode('utf-8')
                image_file.close()
            except Exception:
                pass
        cursor.rewind()

    html = open('./index.html', encoding='utf-8').read()
    template = Template(html)
    email_content = template.render(items=cursor, b64=b64)
    msg.add_header('Content-Type', 'text/html')
    msg.set_payload(email_content)

    server = smtplib.SMTP(cfg['email']['smtp_server'])
    server.starttls()
    server.login(msg['From'], password)
    server.sendmail(msg['From'], (cfg['email']['to']).split(','), msg.as_string().encode('utf-8'))


def main():
    try:
        print(f'Отправка... ({pendulum.now().to_datetime_string()})')
        cursor = get_records(config)
        send_email(cursor, config)
        print('Успешно!')
    except Exception as e:
        print(str(e))


def countdown():
    print(
        f"Отправка писем на [{config['email']['to']}] в [{config['other']['start_time']}] сейчас [{pendulum.now().to_time_string()[:-3]}]")


countdown()
schedule.every().day.at(config['other']['start_time']).do(main)
schedule.every(2).hours.do(countdown)

while True:
    schedule.run_pending()
