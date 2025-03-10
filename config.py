from sqlmesh.core.config import (
    Config,
    ModelDefaultsConfig,
    NameInferenceConfig,
    GatewayConfig,
    MSSQLConnectionConfig
)
from roskarl import env_var_dsn

lakehouse_dsn = env_var_dsn(name="LAKEHOUSE")

config = Config(
    gateways={
        "mssql": GatewayConfig(
            connection=MSSQLConnectionConfig(
                user=lakehouse_dsn.username,
                host=lakehouse_dsn.hostname,
                password=lakehouse_dsn.password,
                port=lakehouse_dsn.port,
                database=lakehouse_dsn.database,
                charset='cp1252'
            )
        )
    },
    default_gateway="mssql",
    model_naming=NameInferenceConfig(
        infer_names=True
    ),
    model_defaults=ModelDefaultsConfig(
        dialect="tsql",
        start="2025-03-07"
    )
)