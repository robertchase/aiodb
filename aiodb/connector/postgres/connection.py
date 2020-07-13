"""action routines for postgres connection FSM"""
import hashlib

import aiodb.connector.postgres.constants as constants
import aiodb.connector.postgres.message as message


class Context:  # pylint: disable=too-few-public-methods
    """FSM context"""
    # pylint: disable=too-many-instance-attributes

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
        """return a tuple of (columns, rows)"""
        columns = [d.name for d in self.row_description]
        return columns, self.rows


def act_md5(context, msg):
    """md5 action routine"""
    data = context.password + context.user
    hashed = 'md5' + hashlib.md5(
        hashlib.md5(
            data.encode()
        ).hexdigest().encode() + msg.salt
    ).hexdigest()
    context.send(message.serialize(constants.TAG_PASSWORD, hashed))


def act_parameter(context, msg):
    """parameter action routine"""
    context.parameter[msg.key] = msg.value


def act_backend_key_data(context, msg):
    """backend key data action routine"""
    context.backend_process_id = msg.process_id
    context.backend_secret_key = msg.secret_key


def act_connected(context, msg):  # pylint: disable=unused-argument
    """connected action routine"""
    context.is_connected = True


def act_query(context, query):
    """query action routine"""
    context.is_running = True
    context.row_description = []
    context.rows = []
    context.send(message.serialize(constants.TAG_QUERY, query))


def act_description(context, msg):
    """description action routine"""
    context.row_description = msg.columns


def act_row(context, msg):
    """row action routine"""
    context.rows.append([
        d.convert(v) for d, v in zip(
            context.row_description,
            msg.columns,
        )
    ])


def act_complete(context, msg):  # pylint: disable=unused-argument
    """complete action routine"""
    context.is_running = False


def act_startup(context):
    """startup action routine"""
    context.send(
        message.startup(context.user, context.database)
    )


def act_error(context, msg):
    """error action routine"""
    raise Exception(msg)
