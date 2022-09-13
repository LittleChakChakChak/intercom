import requests
import pandas as pd
import datetime
from langdetect import detect
import asyncio
import aiohttp
import smtplib

# пути к файлам
FILE_WAY = r'./intercom.csv'
LOGS = r'./logs.txt'

# потоков
STREAMS = 4
error = ''

# Разделение загрузки страниц на потоки
def separation_streams(min_page, max_page):
    i = 1
    tasks = []
    min_page = int(min_page)
    max_page = int(max_page)
    sum_pages = max_page - min_page + 1
    period = int(sum_pages / STREAMS)  # страниц за поток

    ioloop = asyncio.get_event_loop()

    if period > 0:
        # основное
        stack_1 = STREAMS * period  # число четных загруженных
        while i <= STREAMS:
            max_page_task = min_page + period - 1
            # ioloop = asyncio.get_event_loop()
            tasks.append(ioloop.create_task(working_data(url, headers, params, columns, check, id_appeal, min_page,
                                                         max_page_task, error)))
            min_page += period
            i += 1

        # ioloop.run_until_complete(asyncio.wait(tasks))

        # дополнительно
        stack_2 = sum_pages - stack_1  # страницы, которые не загрузились в основлной куче
        i = 1
        while i <= stack_2:
            # ioloop = asyncio.get_event_loop()
            tasks.append(
                ioloop.create_task(working_data(url, headers, params, columns, check, id_appeal, min_page, min_page,
                                                error)))
            min_page += 1
            i += 1

        ioloop.run_until_complete(asyncio.wait(tasks))
    else:
        # дозапись если меньше 20 страниц
        i = 1
        while i <= sum_pages:
            # ioloop = asyncio.get_event_loop()
            tasks.append(
                ioloop.create_task(working_data(url, headers, params, columns, check, id_appeal, min_page, min_page,
                                                error)))
            min_page += 1
            i += 1

        ioloop.run_until_complete(asyncio.wait(tasks))

# Поиск не загруженных страниц
def search_lost_pages():
    missing_pages = []

    df = pd.read_csv(FILE_WAY, sep='^', low_memory=False)
    df = df.sort_values(by=['page'])
    pages = df.page.unique()

    for i in range(len(pages) - 1):
        if pages[i] + 1 != pages[i + 1]:
            missing_pages.append(pages[i] + 1)
        if pages[i] - 1 != pages[i - 1]:
            missing_pages.append(pages[i] - 1)
    missing_pages = missing_pages[1:]

    return missing_pages

# Уведомление на почту
def mail(page = 0, last_page = 0, flag = False):
    # От кого:
    fromaddr = 'Pitonyashka <>'

    # Кому:
    toaddr = 'Analytic team <>'

    #Тема письма:
    subj = f'Intercom - {flag} - Pages {page} from {last_page} in {str(datetime.datetime.now())}'

    #Текст сообщения:
    msg_txt = f'{flag} - loading intercom \n\n Pages worked out {page} from {last_page} in {str(datetime.datetime.now())}'

    #Создаем письмо (заголовки и текст)
    msg = "From: %s\nTo: %s\nSubject: %s\n\n%s" % (fromaddr, toaddr, subj, msg_txt)

    #Логин gmail аккаунта. Пишем только имя ящика.
    #Например, если почтовый ящик someaccount@gmail.com, пишем:
    username = ''

    #Соответственно, пароль от ящика:
    password = ''

    #Инициализируем соединение с сервером gmail по протоколу smtp.
    server = smtplib.SMTP('smtp.gmail.com:587')

    #Выводим на консоль лог работы с сервером (для отладки)
    server.set_debuglevel(1)

    #Переводим соединение в защищенный режим (Transport Layer Security)
    server.starttls()

    # Проводим авторизацию:
    server.login(username, password)

    # Отправляем письмо:
    server.sendmail(fromaddr, toaddr, msg)

    # Закрываем соединение с сервером
    server.quit()

# Получение данных о admin из teammates
async def data_admins(id):
    url = f"https://api.intercom.io/admins/{id}?"
    headers = {'Authorization': 'secretKey',
               'Accept': 'application/json'}

    # отправляем запрос
    async with aiohttp.ClientSession() as session:  # [3]
        async with session.get(url, headers=headers, params=params) as resp:  # [4]
            data_admins = await resp.json()

    return {'name': data_admins.get('name', ''), 'email': data_admins.get('email', '')}

# Конвертация времени
def time_convert(time):
    if time != None:
        ts = int(time)
        return datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    else:
        return None

# Получение данных о контакте по id
async def data_contacs(id):
    url = f"https://api.intercom.io/contacts/{id}?"
    headers = {'Authorization': 'secretKey',
               'Accept': 'application/json'}

    # отправляем запрос
    async with aiohttp.ClientSession() as session:  # [3]
        async with session.get(url, headers=headers, params=params) as resp:  # [4]
            data_contacs = await resp.json()

            location = data_contacs.get('location', '')

            custom_attributes = data_contacs.get('custom_attributes', '')

            if type(location) != type('str') and type(custom_attributes) != type('str'):
                return location.get('country', ''), custom_attributes.get('currency', '')
            else:
                return '', ''

# Загрузка/запись данных
async def working_data(url, headers, params, columns, check, id_appeal, page, total_page, error):
    # пробегаем каждую страницу
    while page <= total_page:
        params['page'] = page
        data = []

        # получаем данные постранично
        async with aiohttp.ClientSession() as session:  # [3]
            async with session.get(url, headers=headers, params=params) as resp:  # [4]
                conversations = await resp.json()

                try:
                    conversations = conversations['conversations']
                except:
                    # запись в логи
                    error = f'Ошибка {conversations}в {str(datetime.datetime.now())}\n'
                    print(f'Ошибка {conversations}в {str(datetime.datetime.now())}\n')
                    with open(LOGS, 'a+') as file:
                        file.writelines(
                            f'Ошибка {conversations}в {str(datetime.datetime.now())}\n')


                # читаем каждую запись и записываем как отдельная строка
                for record in conversations:

                    # проверка
                    if int(id_appeal) == int(record['id']) and not check:
                        check = True
                        continue
                    else:
                        if check:


                            # готовим структуру для красивой записи

                            # структуры из запроса
                            source = record.get('source', '')
                            author = source.get('author', '')
                            statistics = record.get('statistics', '')
                            teammates = record.get('teammates', '')
                            custom_attributes = record.get('custom_attributes', '')
                            conversation_rating = record.get('conversation_rating', '')

                            # body
                            body_text = source.get('body', '')

                            # проверка отдельных значений
                            # чистка body от html элементов
                            if body_text.find('<p>') != -1:
                                body_text = body_text.replace('<p>', '')
                                body_text = body_text.replace('</p>', '')
                            if body_text.find('<br>') != -1:
                                body_text = body_text.replace('<br>', ' ')
                            if body_text.find('<div class="intercom-container"><img src="') != -1:
                                body_text = body_text.replace('<div class="intercom-container"><img src="', '')
                            if body_text.find('</div>') != -1:
                                body_text = body_text.replace('</div>', '')
                            if body_text.find(';') != -1:
                                body_text = body_text.replace(';', '')
                            if body_text.find('\n') != -1:
                                body_text = body_text.replace('\n', '')

                            # admins
                            if teammates['admins'] != []:
                                admins = await data_admins(teammates['admins'][0]['id'])
                                first_responder = admins.get('name', '')
                                first_responder_email = admins.get('email', '')
                            else:
                                first_responder = ''
                                first_responder_email = ''

                            if len(teammates['admins']) > 0:
                                admins = await data_admins(teammates['admins'][-1]['id'])
                                last_responder = admins.get('name', '')
                                last_responder_email = admins.get('email', '')
                            else:
                                last_responder = ''
                                last_responder_email = ''

                            # conversation_rating
                            if conversation_rating != None:
                                conversation_rating_rating = conversation_rating.get('rating', '')
                                conversation_rating_remark = conversation_rating.get('remark', '')
                                if conversation_rating_remark != None:
                                    if conversation_rating_remark.find('\n') != -1:
                                        conversation_rating_remark = conversation_rating_remark.replace('\n', '')
                            else:
                                conversation_rating_rating = ''
                                conversation_rating_remark = ''

                            # определение языка по body
                            if body_text != '' or body_text.find('http://') != -1:
                                try:
                                    lang = detect(body_text)
                                except:
                                    lang = ''
                            else:
                                lang = ''

                            # получение данных последнего закрывающего
                            if statistics.get('last_closed_by_id', '') != None:
                                last_closed_by = await data_admins(statistics['last_closed_by_id'])
                                last_closed_by_name = last_closed_by.get('name', '')
                                last_closed_by_email = last_closed_by.get('email', '')
                            else:
                                last_closed_by_name = ''
                                last_closed_by_email = ''

                            # перевод времи в минуты
                            time_to_assignment = statistics.get('time_to_assignment', '')
                            if time_to_assignment != '' and time_to_assignment != 0 and time_to_assignment != None:
                                time_to_assignment = time_to_assignment / 60
                               
                            time_to_admin_reply = statistics.get('time_to_admin_reply', '')
                            if time_to_admin_reply != '' and time_to_admin_reply != 0 and time_to_admin_reply != None:
                                time_to_admin_reply = time_to_admin_reply / 60

                            time_to_first_close = statistics.get('time_to_first_close', '')
                            if time_to_first_close != '' and time_to_first_close != 0 and time_to_first_close != None:
                                time_to_first_close = time_to_first_close / 60

                            time_to_last_close = statistics.get('time_to_last_close', '')
                            if time_to_last_close != '' and time_to_last_close != 0 and time_to_last_close != None:
                                time_to_last_close = time_to_last_close / 60

                            median_time_to_reply = statistics.get('median_time_to_reply', '')
                            if median_time_to_reply != '' and median_time_to_reply != 0 and median_time_to_reply != None:
                                median_time_to_reply = median_time_to_reply / 60

                            location, currency = await data_contacs(author.get('id', ''))

                            # строка данных

                            line_data = [page, record.get('id', ''), record.get('admin_assignee_id', ''),
                                         record.get('team_assignee_id', ''), record.get('state', ''),
                                         record.get('title', ''),

                                         # custom_attributes
                                         custom_attributes.get('Type', ''), custom_attributes.get('Result', ''),

                                         # source
                                         author.get('id', ''), author.get('name', ''), author.get('type', ''),
                                         body_text, lang,

                                         # conversation_rating
                                         conversation_rating_rating, conversation_rating_remark,

                                         # teammates
                                         teammates.get('admins', ''),

                                         # statistics
                                         time_to_assignment,
                                         time_to_admin_reply,
                                         time_to_first_close,
                                         time_to_last_close,
                                         median_time_to_reply,
                                         time_convert(statistics.get('first_contact_reply_at', '')),
                                         time_convert(statistics.get('first_assignment_at', '')),
                                         time_convert(statistics.get('first_admin_reply_at', '')),
                                         time_convert(statistics.get('first_close_at', '')),
                                         time_convert(statistics.get('last_assignment_at', '')),
                                         time_convert(statistics.get('last_assignment_admin_reply_at', '')),
                                         time_convert(statistics.get('last_contact_reply_at', '')),
                                         time_convert(statistics.get('last_admin_reply_at', '')),
                                         time_convert(statistics.get('last_close_at', '')),
                                         statistics.get('last_closed_by_id', ''),
                                         last_closed_by_name,
                                         last_closed_by_email,
                                         statistics.get('count_reopens', ''),
                                         statistics.get('count_assignments', ''),
                                         statistics.get('count_conversation_parts', ''),
                                         # admins
                                         first_responder, first_responder_email,
                                         last_responder, last_responder_email,

                                         # location
                                         location, currency,

                                         time_convert(record.get('updated_at', ''))]

                            # добавление записи в массив
                            data.append(line_data)

                        else:
                            continue

                df = pd.DataFrame(data, columns=columns)

                # запись в excel файл
                df.to_csv(FILE_WAY, index=False, mode='a', header=False, sep='^')

                print(f"Страниц отработано {page} из {total_page} в {str(datetime.datetime.now())}")

                # переход на следующую страницу
                page += 1

            # запись в логи
            with open(LOGS, 'a+') as file:
                file.writelines(
                    f'Страниц загружено {page - 1} из {total_page}\n Файл заполнен в {str(datetime.datetime.now())}\n')

try:
    # Адрес api
    url = "https://api.intercom.io/conversations/?"
    headers = {'Authorization': 'secretKey',
               'Accept': 'application/json'}
    params = {'per_page': 60, 'order': 'asc'}

    # колонки в файл
    columns = ['page', 'id_appeal', 'admin_assignee_id', 'team_assignee_id', 'state', 'title',

                # custom_attributes
                'custom_attributes_type', 'custom_attributes_result',

                # source
                'source_author_id', 'source_author_email', 'source_author_type', 'source_body', 'lang',

                # conversation_rating
                'conversation_rating_rating', 'conversation_rating_remark',

                # teammates
                'teammates_admins',

                # statics
                'time_to_assignment', 'time_to_admin_reply', 'time_to_first_close', 'time_to_last_close',
                'median_time_to_reply', 'first_contact_reply_at', 'first_assignment_at', 'first_admin_reply_at',
                'first_close_at', 'last_assignment_at', 'last_assignment_admin_reply_at',
                'last_contact_reply_at', 'last_admin_reply_at', 'last_close_at', 'last_closed_by_id',
                'last_closed_by_name', 'last_closed_by_email',
                'count_reopens', 'count_assignments', 'count_conversation_parts',

                # teammates
                'first_responder', 'first_responder_email', 'last_responder', 'last_responder_email',

                'location',
                'currency',

                'updated_at']

    check = False

    # просмотр последней записи
    id_appeal = int()
    page = 1

    try:
        df = pd.read_csv(FILE_WAY, sep='^',  low_memory=False)
        page = max(df.page)
        id_appeal = max(df.id_appeal)
        check = True
    except:
        df = pd.DataFrame(columns=columns)
        df.to_csv(FILE_WAY, index=False, sep='^')
        check = True

    # отправляем общий запрос
    response = requests.get(url, headers=headers, params=params)
    # получаем данные
    conversations = response.json()

    # всего страниц
    total_page = conversations['pages']['total_pages']

    # ------------------------------------------------------------------------------------------------------

    # Запуск ассинхроности для догрузки/загрузки страниц
    separation_streams(page, total_page)

    # Удаление дублей после дозаписи
    df = pd.read_csv(FILE_WAY, sep='^', low_memory=False)
    df = df.sort_values(by=['page'])
    df['duble'] = df.duplicated(subset='id_appeal', keep='last')
    # получаем страницу с дублями
    df_duble = df.query('duble == True')
    pages_duble = df_duble['page'].unique()
    # удаляем страницы с дублями
    for page_duble in pages_duble:
        df = df.query('page != @page_duble')

    df.to_csv(FILE_WAY, index=False, mode='w', sep='^')

    # доскачиваем страницы, которые были удалены
    i = 0
    tasks = []

    ioloop = asyncio.get_event_loop()

    p = int(len(pages_duble)/STREAMS) + 1
    p1 = 0
    p2 = STREAMS
    while i <= p:
        for page_d in pages_duble[p1:p2]:
            tasks.append(ioloop.create_task(working_data(url, headers, params, columns, check, id_appeal,
                                                     int(page_d), int(page_d), error)))
        i += 1
        p1 += STREAMS
        p2 += STREAMS

    ioloop.run_until_complete(asyncio.wait(tasks))

    # доскачиваем страницы, которые были удалены (ошибки времени запросов)
    missing_pages = search_lost_pages()

    i = 0
    while i < len(missing_pages):
        separation_streams(missing_pages[i], missing_pages[i + 1])
        i += 2

    # запись в логи (конец)
    with open(LOGS, 'a+') as file:
        file.writelines(f'Страниц загружено {page} из {total_page}\n Файл заполнен в {str(datetime.datetime.now())}\n')

    print(f'Happy and! {str(datetime.datetime.now())}')
    if error != '':
        mail(0, 0, error)
    else:
        mail(page, total_page, True)
except:
    print('Ошибочка)')
    mail(0, 0, False)

# input()