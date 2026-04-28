
import import_declare_test

from splunktaucclib.rest_handler.endpoint import (
    field,
    validator,
    RestModel,
    MultipleModel,
)
from splunktaucclib.rest_handler import admin_external, util
from splunktaucclib.rest_handler.admin_external import AdminExternalHandler
import logging

util.remove_http_proxy_env_vars()


fields_updatecribllookup_proxy = [
    field.RestField(
        'verify_ssl',
        required=False,
        encrypted=False,
        default=True,
        validator=None
    ), 
    field.RestField(
        'proxy_enabled',
        required=False,
        encrypted=False,
        default=False,
        validator=None
    ), 
    field.RestField(
        'proxy_type',
        required=False,
        encrypted=False,
        default='http',
        validator=validator.AllOf(
            validator.String(
                max_len=10, 
                min_len=1, 
            ), 
            validator.Pattern(
                regex=r"""^(http|socks4|socks5)$""", 
            )
        )
    ), 
    field.RestField(
        'proxy_url',
        required=False,
        encrypted=False,
        default=None,
        validator=validator.String(
            max_len=4096, 
            min_len=0, 
        )
    ), 
    field.RestField(
        'proxy_port',
        required=False,
        encrypted=False,
        default=None,
        validator=validator.Number(
            max_val=65535, 
            min_val=1, 
        )
    ), 
    field.RestField(
        'proxy_username',
        required=False,
        encrypted=False,
        default=None,
        validator=validator.String(
            max_len=255, 
            min_len=0, 
        )
    ), 
    field.RestField(
        'proxy_password',
        required=False,
        encrypted=True,
        default=None,
        validator=validator.String(
            max_len=8192, 
            min_len=0, 
        )
    ), 
    field.RestField(
        'proxy_rdns',
        required=False,
        encrypted=False,
        default=False,
        validator=None
    )
]
model_updatecribllookup_proxy = RestModel(fields_updatecribllookup_proxy, name='updatecribllookup_proxy')


fields_updatecribllookup_logging = [
    field.RestField(
        'loglevel',
        required=True,
        encrypted=False,
        default='INFO',
        validator=validator.Pattern(
            regex=r"""^INFO|DEBUG|ERROR$""", 
        )
    )
]
model_updatecribllookup_logging = RestModel(fields_updatecribllookup_logging, name='updatecribllookup_logging')


endpoint = MultipleModel(
    'updatecribllookup_settings',
    models=[
        model_updatecribllookup_proxy, 
        model_updatecribllookup_logging
    ],
    need_reload=False,
)


if __name__ == '__main__':
    logging.getLogger().addHandler(logging.NullHandler())
    admin_external.handle(
        endpoint,
        handler=AdminExternalHandler,
    )
