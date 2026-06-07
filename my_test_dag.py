# Найденные ошибки и неточности
# 1. Лишняя скобка перед def hello_world: в тексте есть } перед функцией.
# 2. Неверный отступ у print внутри функции hello_world.
# 3. Неиспользуемый импорт datetime: для практики в параметре start_date попробуем вместо days_ago использовать datetime.
# 4. Функция hello_world должна быть определена до её использования в PythonOperator.

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

def hello_world():
    print('Hello Airflow from Python')

# Функция для новой Task
def goodbye_world():
    print('Goodbye from Python Task')

default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "my_test_dag",
    default_args=default_args,
    description="My test DAG with fixes",
    schedule=None,
    start_date=datetime(2026, 5, 13),   # используем datetime для указания конкретной даты вместо days_ago 
    catchup=False,
    tags=["test", "sales"],
) as dag:

    bash_first = BashOperator(
        task_id='bash_first',
        bash_command='echo "Hello Airflow from Bash (Start)"'
    )

    bash_middle = BashOperator(
        task_id='bash_middle',
        bash_command='echo "Parallel Bash task"'
    )

    python_1 = PythonOperator(
        task_id='python_1',
        python_callable=hello_world
    )

    python_2 = PythonOperator(
        task_id='python_2',
        python_callable=hello_world
    )

    # Дополнительный созданный Task с PythonOperator
    python_3 = PythonOperator(
        task_id='python_3',
        python_callable=goodbye_world
    )

    # Дополнительный созданный Task с BashOperator
    bash_last = BashOperator(
        task_id='bash_last',
        bash_command='echo "Bye Airflow from Bash (End)"'
    )

    bash_first >> [python_1, bash_middle]
    python_1 >> python_3
    bash_middle >> python_2
    [python_2, python_3] >> bash_last

    # Логика:
    # 1. Сначала выполняется bash_first.
    # 2. Затем параллельно запускаются python_1 и bash_middle.
    # 3. После python_1 запускается python_3.
    # 4. После bash_middle запускается python_2.
    # 5. И только когда python_2 и python_3 оба отработают, выполняется bash_last.