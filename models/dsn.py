from dataclasses import dataclass
from urllib.parse import unquote
from typing import Optional
import re

@dataclass
class DSN:
    protocol: str
    username: str
    password: str
    hostname: str
    port: Optional[int]
    database: Optional[str]

    def __str__(self) -> str:
        """Return the DSN as a string, with the password masked"""
        port_str = f":{self.port}" if self.port else ""
        db_str = f"/{self.database}" if self.database else ""
        return f"{self.protocol}://{self.username}:****@{self.hostname}{port_str}{db_str}"

    def to_dict(self) -> dict:
        """Convert to dictionary format"""
        return {
            'protocol': self.protocol,
            'username': self.username,
            'password': self.password,
            'hostname': self.hostname,
            'port': self.port,
            'database': self.database
        }

def parse_dsn(dsn_string: str) -> DSN:
    try:
        # Extract protocol
        protocol_match = re.match(r'^([^:]+)://', dsn_string)
        if not protocol_match:
            raise ValueError('Invalid DSN: Protocol not found')

        protocol = protocol_match.group(1)
        remaining = dsn_string[len(protocol_match.group(0)):]

        # Find last @ to separate credentials from host info
        last_at = remaining.rindex('@')
        credentials = remaining[:last_at]
        host_part = remaining[last_at + 1:]

        # Parse credentials
        first_colon = credentials.index(':')
        username = unquote(credentials[:first_colon])
        password = unquote(credentials[first_colon + 1:])

        # Parse host info
        database_parts = host_part.split('/', 1)
        host_and_port = database_parts[0]
        database = database_parts[1] if len(database_parts) > 1 else None

        # Parse hostname and port
        if ':' in host_and_port:
            hostname, port_str = host_and_port.split(':')
            port = int(port_str)
        else:
            hostname = host_and_port
            port = None

        return DSN(
            protocol=protocol,
            username=username,
            password=password,
            hostname=hostname,
            port=port,
            database=database
        )

    except ValueError as e:
        raise ValueError(f'Failed to parse DSN string: {str(e)}')
    except Exception as e:
        raise ValueError(f'Failed to parse DSN string: Unexpected error - {str(e)}')


# # Test cases
# if __name__ == "__main__":
#     test_cases = [
#         # Basic case
#         'postgresql://user:pass@localhost:5432/mydb',

#         # Password with special characters
#         'mysql://user:p@ssw:rd@localhost:3306/mydb',

#         # URL-encoded special characters
#         'postgresql://user:complex%40pass%3Award@localhost:5432/mydb',

#         # No port specified
#         'mysql://user:pass@localhost/mydb',

#         # No database specified
#         'postgresql://user:pass@localhost:5432'
#     ]

#     for i, dsn in enumerate(test_cases, 1):
#         print(f"\nTest case {i}:")
#         print(f"Input: {dsn}")
#         try:
#             result = parse_dsn(dsn)
#             print("Result (safe print):", result)
#             print("As dictionary:", result.to_dict())
#         except ValueError as e:
#             print("Error:", str(e))