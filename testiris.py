import iris

host = "localhost"
port = 1972
namespace = "USER"
username = "_SYSTEM"
password = "SYS"

conn = iris.connect(
    host,
    port,
    namespace,
    username,
    password,
)

with conn.cursor() as cursor:
    cursor = conn.cursor()

    res = cursor.execute("DROP TABLE IF EXISTS test")
    res = cursor.execute(
        """
    CREATE TABLE test (
            id IDENTITY NOT NULL,
            value VARCHAR(50)
    ) WITH %CLASSPARAMETER ALLOWIDENTITYINSERT = 1
    """
    )

    cursor = conn.cursor()
    res = cursor.executemany(
        "INSERT INTO test (id, value) VALUES (?, ?)", [
            (1, 'val1'),
            (2, 'val2'),
            (3, 'val3'),
            (4, 'val4'),
        ]
    )
    print(res)

# # cursor = conn.cursor()
# # res = cursor.executemany(
# #     "INSERT INTO test DEFAULT VALUES", [
# #         None,
# #         None,
# #         None,
# #         None,
# #     ]
# # )

    res = cursor.executemany(
        "INSERT INTO test (value) VALUES (?)", [
            'val1',
            'val2',
            'val3',
            'val4',
        ]
    )
#     print("res", res)
