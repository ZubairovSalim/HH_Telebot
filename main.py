import telebot
import requests
import time
import config
import databases
import datetime
from multiprocessing import Process
from threading import Thread

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

bot = telebot.TeleBot(config.TOKEN)

hh_api_url = 'https://api.hh.ru/vacancies'

engine = create_engine(config.DB_URI)
databases.Base.metadata.create_all(bind=engine)

Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

processes = {}

keyboard1 = telebot.types.ReplyKeyboardMarkup(True)
keyboard1.row('/start', '/stop')

@bot.message_handler(commands=['start'])
def start_message(message):
    delete_saved_chat(message.chat.id)
    new_row = databases.Chat_Table(chat_id=message.chat.id)
    session.add(new_row)
    session.commit()
    bot.send_message(message.chat.id, 'Привет, я hh bot! Могу помочь с поиском вакансии :)\n' +
                     'Напиши ключевое слово: ', reply_markup=keyboard1)


@bot.message_handler(commands=['stop'])
def start_message(message):
    delete_saved_chat(message.chat.id)


@bot.message_handler(content_types=['text'])
def text_message(message):
    row = session.query(databases.Chat_Table).filter(databases.Chat_Table.chat_id == message.chat.id).first()
    if not row:
        bot.send_message(message.chat.id,
                         'Для создания нового поиска вакансий введите команду /start')
        return
    if row.text is None and row.salary is None and row.employment is None:
        row.text = message.text
        session.commit()
        bot.send_message(message.chat.id, 'Напиши желаемый оклад')
    elif row.salary is None and row.employment is None:
        if message.text.isdigit():
            row.salary = message.text
            session.commit()
            bot.send_message(message.chat.id, 'Напиши тип занятости')
        else:
            bot.send_message(message.chat.id, 'Неверно введенные данные')
    elif row.employment is None:
        if message.text.find('Полная') != -1:
            row.employment = 'full'
        elif message.text.find('Частичная') != -1:
            row.employment = 'part'
        elif message.text.find('Проектная') != -1:
            row.employment = 'project'
        elif message.text.find('Волонтер') != -1:
            row.employment = 'volunteer'
        elif message.text.find('Стажировка') != -1:
            row.employment = 'probation'
        if row.employment is None:
            bot.send_message(message.chat.id,
                             'Введите пожалуйста значения типа: (Полная,Частичная,Проектная,Волонтерство,Стажировка)')
        else:
            session.commit()
            bot.send_message(message.chat.id, 'Всё понял! Начинаю поиск!')
            create_process(row.id)


def delete_saved_chat(chat_id):
    row = session.query(databases.Chat_Table).filter(databases.Chat_Table.chat_id == chat_id).first()
    if row:
        if row.id in processes.keys():
            delete_process(row.id)
        session.delete(row)
        session.commit()


def create_process(id):
    processes[id] = Process(target=send_vacancies, args=(id, ), daemon=True)
    processes[id].start()


def delete_process(id):
    processes[id].terminate()
    processes.pop(id, None)


def watchdog():
    while True:
        for row in session.query(databases.Chat_Table):
            if row.id in processes.keys() and not processes[row.id].is_alive():
                delete_saved_chat(row.chat_id)
                print('Process for chat %d was terminated. Chat was deleted.' % row.chat_id)
        time.sleep(10)


def get_new_vacancies(id, max_count=50):
    chat = session.query(databases.Chat_Table).filter(databases.Chat_Table.id == id).first()
    if not chat or not chat.text or not chat.salary or not chat.employment:
        return None

    date_from = None if chat.last_update is None else chat.last_update.astimezone().isoformat()
    per_page = min(max_count, 100)
    page_count = (max_count + per_page - 1) // per_page

    params = {'text': chat.text,
              'salary': str(round(chat.salary)),
              'employment': chat.employment,
              'order_by': 'publication_time',
              'per_page': per_page}
    if date_from is not None:
        params['date_from'] = date_from

    vacs = []
    found = 0
    for i in range(page_count):
        params['page'] = i
        r = requests.get(hh_api_url, params=params)
        e = r.json()
        vacs += e['items']
        found += e['found']
    vacs = vacs[:min(max_count, found)]

    chat.last_update = datetime.datetime.now().replace(microsecond=0).astimezone()
    session.commit()
    return vacs


def send_vacancies(id):
    chat_id = session.query(databases.Chat_Table)\
                     .filter(databases.Chat_Table.id == id)\
                     .first().chat_id
    while True:
        vacs = get_new_vacancies(id)
        if vacs is None:
            return
        for v in vacs:
            try:
                bot.send_message(chat_id, v['alternate_url'])
            except telebot.apihelper.ApiException as e:
                if e.result.status_code == 403:
                    print("Error: Chat %d was deleted by user" % chat_id)
                    return
                elif e.result.status_code == 409:
                    print('Error: Webhook exception. Reset...')
                    bot.delete_webhook()
                    time.sleep(15)
                    print('Done')
                    bot.send_message(chat_id, v['alternate_url'])
                else:
                    print(e)
                    return
            except Exception as e:
                print(e)
                return
            time.sleep(10)
        time.sleep(60)


if __name__ == '__main__':
    for chat in session.query(databases.Chat_Table):
        if chat.text and chat.salary and chat.employment:
            create_process(chat.id)

    time.sleep(2)
    wdog = Thread(target=watchdog, daemon=True)
    wdog.start()

    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=20)
        except telebot.apihelper.ApiException as e:
            if e.result.status_code == 409:
                print('Error: Webhook exception. Reset...')
                bot.delete_webhook()
                time.sleep(15)
                print('Done')
            else:
                print(e)
                time.sleep(15)
        except Exception as e:
            print(e)
            time.sleep(15)

