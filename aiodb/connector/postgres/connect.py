import asyncio
import os

from db import DB


async def main(query, *args):
    db = DB(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', 5432),
        user=os.getenv('POSTGRES_USER', 'foo'),
        password=os.getenv('POSTGRES_PASSWORD', ''),
        database='test',
        # debug=True,
    )

    cursor = await db.cursor()
    await cursor.start_transaction()
    result = await cursor.execute(query, args)
    await cursor.commit()
    await cursor.close()

    return result

if __name__ == '__main__':
    import logging

    logging.basicConfig(level=logging.DEBUG)

    print(asyncio.run(main('SELECT * FROM test CROSS JOIN (VALUES (1), (2), (3)) AS Z')))
