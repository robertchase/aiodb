import hashlib

import aiodb.connector.postgres.message as message


class Context:

    def __init__(self, send, user=None, password=None, database=None):
        self.send = send
        self.user = user
        self.password = password
        self.database = database
        self.parameter = {}
        self.query = None

        self.row_description = []
        self.rows = []
        self.is_connected = False
        self.is_running = False

    @property
    def result_set(self):
        columns = [d.name for d in self.row_description]
        return columns, self.rows


def act_md5(context, msg):
    data = context.password + context.user
    hash = 'md5' + hashlib.md5(
        hashlib.md5(
            data.encode()
        ).hexdigest().encode() + msg.salt
    ).hexdigest()
    context.send(message.serialize(message.TAG_PASSWORD, hash))


def act_parameter(context, msg):
    context.parameter[msg.key] = msg.value


def act_backend_key_data(context, msg):
    context.backend_process_id = msg.process_id
    context.backend_secret_key = msg.secret_key


def act_connected(context, msg):
    context.is_connected = True


def act_query(context, query):
    context.is_running = True
    context.row_description = []
    context.rows = []
    context.send(message.serialize(message.TAG_QUERY, query))


def act_description(context, msg):
    context.row_description = msg.columns


def act_row(context, msg):
    # print('ROW', msg.columns)
    # print('DESCRIPTION', context.row_description)
    '''
    context.rows.append({
        d.name: d.convert(v) for d, v in zip(
            context.row_description,
            msg.columns,
        )
    })
    '''
    context.rows.append([
        d.convert(v) for d, v in zip(
            context.row_description,
            msg.columns,
        )
    ])


def act_complete(context, msg):
    context.is_running = False


def act_startup(context):
    context.send(
        message.startup(context.user, context.database)
    )


def act_error(context, msg):
    raise Exception(msg)
