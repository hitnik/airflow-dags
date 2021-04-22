from airflow.decorators import dag, task
from airflow.utils.dates import days_ago, timedelta
import requests
import pickle
import os
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from babel.dates import format_date
from airflow.operators import email
from jinja2 import Template
import pendulum

local_tz = pendulum.timezone(os.environ.get('TIMEZONE'))

default_args = {
    'owner': os.environ.get('OWNER'),
    'email_on_failure': True,
    'email': os.environ.get('ALERT_EMAIL'),
    'retries': 1,
    'depends_on_past': False,
}

@dag(
    default_args=default_args,
    schedule_interval=timedelta(days=int(os.environ.get('INTERVAL'))),
    start_date=days_ago(1,
                        int(os.environ.get('START_HOUR')),
                        int(os.environ.get('START_MINUTE'))),
    catchup=False,
    tags=['topics_notify'],
)
def notify_flow():
    """
        DAG for scheduled sending email notifications of
        yesterday topics about Beltelecom.
        DAG has 4 tasks:
           -  get_recipients :param None
                            :return tuple of recipients
           - get_topics_count :param url
                              :return topics_count
           - get_pdf :param url
                     :return pickle of binary pdf
           -send_email :param recips,topics_count, pdf=None
                    :return  None

    """

    @task(default_args=default_args,
          execution_timeout=timedelta(seconds=10))
    def get_recipients():
        """
            Get notification recipients from Topics DB
        :return: tuple
        """
        from sqlalchemy import create_engine, MetaData, Table
        from sqlalchemy.orm import sessionmaker

        SQL_DATABASE = os.environ.get('SQL_DATABASE_REMOTE')
        SQL_USER = os.environ.get('SQL_USER_REMOTE')
        SQL_PASSWORD = os.environ.get('SQL_PASSWORD_REMOTE')
        SQL_HOST = os.environ.get('SQL_HOST_REMOTE')
        SQL_PORT = os.environ.get('SQL_PORT_REMOTE')

        DB_STRING = 'postgresql+psycopg2://' + SQL_USER + ':' + SQL_PASSWORD + '@' + SQL_HOST + '/' + SQL_DATABASE

        engine = create_engine(DB_STRING, encoding='utf-8', echo=False)
        metadata = MetaData(engine)
        metadata.reflect(only=['forumTopics_employees'])
        table = Table('forumTopics_employees', metadata, autoload=True)
        Session = sessionmaker(bind=engine)
        session = Session()
        result = session.query(table.c.lastName, table.c.firstName,
                                   table.c.patronymic, table.c.email
                                   ).filter(table.c.isActive == '1').all()
        session.close()
        if len(result) < 1:
            raise Exception('No active recipients')
        return tuple(result)

    @task(default_args=default_args,
          execution_timeout=timedelta(seconds=10))
    def get_topisc_count(url):
        """
            Get topics count from forums API
        :param url: str
        :return:
        """
        today = pendulum.now(local_tz)
        yesterday = today - timedelta(days=1)
        date_str = yesterday.strftime('%Y-%m-%d')
        req = requests.models.PreparedRequest()
        req.prepare_url(url, {'date': date_str})
        response = requests.get(req.url)
        if response.status_code != 200:
            raise requests.HTTPError
        return response.json()['count']

    @task(default_args=default_args)
    def get_pdf(url):
        """
            create pdf file for attach
        :param url: str
        :return: pickle
        """
        response = requests.get(url)
        if response.status_code != 200:
            raise AirflowSkipException
        return pickle.dumps(response.content)


    @task(default_args=default_args, trigger_rule=TriggerRule.NONE_FAILED)
    def email_send(recips,topics_count, pdf=None):
        """
            Send notification emails to active recipients
        :param recips:
        :param topics_count:
        :param pdf:
        :return:
        """

        with open('/opt/airflow/common/templates/topics_email.html') as f:
            html = f.read()

        template = Template(html)
        today = pendulum.now(local_tz)
        yesterday = today - timedelta(days=1)
        dateString = format_date(yesterday, format='long', locale='ru')
        host = os.environ.get('DJANGO_HOST')
        port = os.environ.get('DJANGO_PORT', '80')
        if port != '80':
            url = 'http://' + host
        else:
            url = 'http://' + host

        subject = 'Отзывы о Белтелеком за ' + yesterday.strftime('%d.%m.%Y')

        topicsCountLastDigit = topics_count % 10
        review = 'отзывов'
        left = 'оставлено'
        if topicsCountLastDigit == 1:
            review = "отзыв"
            left = 'оставлен'
        elif topicsCountLastDigit in [2, 3, 4]:
            review = 'отзыва'

        files_pathes = []
        if pdf:
            p = pickle.loads(pdf)
            with open('forums.pdf', 'wb') as file:
                file.write(pdf)
            files_pathes.append('forums.pdf')

        context = {'left': left,
                   'review': review,
                   'topicsCount': topics_count,
                   'dateString': dateString,
                   'url': url,
                   }

        content = template.render(context)
        for index, recip in enumerate(recips):
            email_operator = email.EmailOperator(
                task_id="send_email_"+str(index),
                to=[recip[3]],
                subject=subject,
                files=files_pathes,
                html_content=content,
            )
            email_operator.execute(context)

    recipients = get_recipients()
    topics_count = get_topisc_count(os.environ.get('URL_TOPICS_COUNT'))
    pdf = get_pdf(os.environ.get('URL_PDF_API'))
    email_send(recipients, topics_count, pdf)

dag = notify_flow()


