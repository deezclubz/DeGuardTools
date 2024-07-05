from datetime import datetime

from pprint import pprint

import aiohttp
import asyncio
import pandas as pd

API_KEY = 'PKWM92TAUUUDB268118A77RPQS7CNVHUCB'
ADDRESS = '0x35A5206D4f58ae3114a356a18c0277dc032a17d5'
BASE_URL = 'https://api.bscscan.com/api'


# Получение текущего номера блока
async def get_latest_block():
    async with aiohttp.ClientSession() as session:
        params = {
            'module': 'proxy',
            'action': 'eth_blockNumber',
            'apikey': API_KEY
        }
        async with session.get(BASE_URL, params=params) as response:
            data = await response.json()
            return int(data['result'], 16)


# Асинхронная функция для получения данных транзакций с BscScan API
async def get_transactions(session, address, start_block, end_block, page=1, offset=1000, sort='asc'):
    params = {
        'module': 'account',
        'action': 'txlist',
        'address': address,
        'startblock': start_block,
        'endblock': end_block,
        'page': page,
        'offset': offset,
        'sort': sort,
        'apikey': API_KEY
    }
    async with session.get(BASE_URL, params=params) as response:
        data = await response.json()
        print(data)
        if data['status'] == '1':
            return {
                "from": data['result'][0]['from'],
                "to": data['result'][0]['to'],
                "transaction_date": datetime.utcfromtimestamp(int(data['result'][0]['timeStamp'])).strftime(
                    '%Y-%m-%d %H:%M:%S'),
                "value_in_bnb": int(data['result'][0]['value']) / 10 ** 18
            }
        else:
            print(f"Error fetching data: {data['message']}")
            return []


# Собираем все транзакции, делая запросы параллельно
async def fetch_all_transactions(address, start_block, end_block):
    all_transactions = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        current_block = start_block
        while current_block <= current_block:
            next_block = min(current_block + 10000, end_block)
            task = asyncio.create_task(get_transactions(session, address, current_block, next_block))
            tasks.append(task)

            current_block = next_block + 1

        results = await asyncio.gather(*tasks)
        for result in results:
            all_transactions.extend(result)
    return all_transactions


async def main():
    start_block = 32427254
    latest_block = await get_latest_block()
    print(f"Fetching transactions from block {start_block} to {latest_block}")

    all_transactions = await fetch_all_transactions(ADDRESS, start_block, latest_block)

    # Конвертация данных в DataFrame
    df = pd.DataFrame(all_transactions)

    # Сохранение данных в CSV
    df.to_csv('bsc_transactions.csv', index=False)

    print("Data fetched and saved successfully.")
    print(await get_latest_block())


if __name__ == '__main__':
    asyncio.run(main())