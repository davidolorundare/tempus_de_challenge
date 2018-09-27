import datetime


def print_context(**context):
    """Print context provided to function."""
    print('Context: {}'.format(context))


class HelloWorld:

    def __call__(self, **context) -> str:
        """Print and return `Hello, <name>!`."""
        hello = 'Hello, {}!'.format(context['params']['name'])
        print(hello)
        return hello


class PrintExecutionDate:
    """Print the execution date as YYYY-MM-DD.

        Note:
            https://airflow.apache.org/code.html#default-variables
    """

    @classmethod
    def callable(cls, **context):
        execution_date = context['ds']
        svc = cls(execution_date)
        return svc.process()

    def __init__(self, execution_date: datetime.datetime):
        self.execution_date = execution_date

    def process(self) -> str:
        execution_date = 'Date: {}'.format(self.execution_date)
        print(execution_date)
        return execution_date
