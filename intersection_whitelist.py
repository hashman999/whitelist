import requests
import json
import pandas as pd
from datetime import datetime
import time

class EnhancedExchangeDataFetcher:
    def __init__(self):
        # CoinGecko API for market cap data
        self.coingecko_url = "https://api.coingecko.com/api/v3/coins/markets"
        
        # Exchange APIs for volume data
        self.binance_url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        self.okx_url = "https://www.okx.com/api/v5/market/tickers?instType=SWAP"
        self.bybit_url = "https://api.bybit.com/v5/market/tickers?category=linear"
        
    def get_market_cap_rankings(self, limit=300):
        """从CoinGecko获取市值排名数据"""
        try:
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': limit,
                'page': 1,
                'sparkline': False
            }
            response = requests.get(self.coingecko_url, params=params, timeout=15)
            data = response.json()
            
            market_cap_data = {}
            for i, coin in enumerate(data, 1):
                symbol = coin['symbol'].upper()
                market_cap_data[symbol] = {
                    'rank': i,
                    'market_cap': coin['market_cap'],
                    'name': coin['name'],
                    'price': coin['current_price']
                }
            
            print(f"获取到 {len(market_cap_data)} 个币种的市值数据")
            return market_cap_data
            
        except Exception as e:
            print(f"CoinGecko API错误: {e}")
            return {}
    
    def get_binance_volume_rankings(self):
        """获取币安24小时交易量排名"""
        try:
            response = requests.get(self.binance_url, timeout=10)
            data = response.json()
            
            volume_data = []
            for item in data:
                if item['symbol'].endswith('USDT'):
                    base_symbol = item['symbol'].replace('USDT', '')
                    volume_data.append({
                        'symbol': base_symbol,
                        'volume_24h': float(item['quoteVolume']),
                        'pair': item['symbol']
                    })
            
            # 按交易量排序
            volume_data.sort(key=lambda x: x['volume_24h'], reverse=True)
            
            volume_rankings = {}
            for i, item in enumerate(volume_data, 1):
                volume_rankings[item['symbol']] = {
                    'rank': i,
                    'volume_24h': item['volume_24h'],
                    'pair': item['pair']
                }
            
            print(f"币安获取到 {len(volume_rankings)} 个交易对的交易量数据")
            return volume_rankings
            
        except Exception as e:
            print(f"币安API错误: {e}")
            return {}
    
    def get_okx_volume_rankings(self):
        """获取OKX24小时交易量排名"""
        try:
            response = requests.get(self.okx_url, timeout=10)
            data = response.json()
            
            volume_data = []
            if data['code'] == '0':
                for item in data['data']:
                    if item['instId'].endswith('-USDT-SWAP'):
                        base_symbol = item['instId'].replace('-USDT-SWAP', '')
                        volume_data.append({
                            'symbol': base_symbol,
                            'volume_24h': float(item['volCcy24h']),
                            'pair': item['instId']
                        })
            
            # 按交易量排序
            volume_data.sort(key=lambda x: x['volume_24h'], reverse=True)
            
            volume_rankings = {}
            for i, item in enumerate(volume_data, 1):
                volume_rankings[item['symbol']] = {
                    'rank': i,
                    'volume_24h': item['volume_24h'],
                    'pair': item['pair']
                }
            
            print(f"OKX获取到 {len(volume_rankings)} 个交易对的交易量数据")
            return volume_rankings
            
        except Exception as e:
            print(f"OKX API错误: {e}")
            return {}
    
    def get_bybit_volume_rankings(self):
        """获取Bybit24小时交易量排名"""
        try:
            response = requests.get(self.bybit_url, timeout=10)
            data = response.json()
            
            volume_data = []
            if data['retCode'] == 0:
                for item in data['result']['list']:
                    if item['symbol'].endswith('USDT'):
                        base_symbol = item['symbol'].replace('USDT', '')
                        volume_data.append({
                            'symbol': base_symbol,
                            'volume_24h': float(item['turnover24h']),
                            'pair': item['symbol']
                        })
            
            # 按交易量排序
            volume_data.sort(key=lambda x: x['volume_24h'], reverse=True)
            
            volume_rankings = {}
            for i, item in enumerate(volume_data, 1):
                volume_rankings[item['symbol']] = {
                    'rank': i,
                    'volume_24h': item['volume_24h'],
                    'pair': item['pair']
                }
            
            print(f"Bybit获取到 {len(volume_rankings)} 个交易对的交易量数据")
            return volume_rankings
            
        except Exception as e:
            print(f"Bybit API错误: {e}")
            return {}

class IntersectionWhitelistGenerator:
    def __init__(self):
        self.fetcher = EnhancedExchangeDataFetcher()
        
    def get_intersection_pairs(self, market_cap_data, volume_data, 
                             market_cap_top, volume_top, exchange_name):
        """获取市值排名和交易量排名的交集"""
        # 获取市值前N名的币种
        market_cap_top_symbols = set()
        for symbol, data in market_cap_data.items():
            if data['rank'] <= market_cap_top:
                market_cap_top_symbols.add(symbol)
        
        # 获取交易量前N名的币种
        volume_top_symbols = set()
        for symbol, data in volume_data.items():
            if data['rank'] <= volume_top:
                volume_top_symbols.add(symbol)
        
        # 计算交集
        intersection = market_cap_top_symbols.intersection(volume_top_symbols)
        
        # 构建详细信息
        intersection_details = []
        for symbol in intersection:
            if symbol in market_cap_data and symbol in volume_data:
                detail = {
                    'symbol': f"{symbol}/USDT",
                    'base_symbol': symbol,
                    'market_cap_rank': market_cap_data[symbol]['rank'],
                    'volume_rank': volume_data[symbol]['rank'],
                    'market_cap': market_cap_data[symbol]['market_cap'],
                    'volume_24h': volume_data[symbol]['volume_24h'],
                    'exchange': exchange_name
                }
                intersection_details.append(detail)
        
        # 按市值排名排序
        intersection_details.sort(key=lambda x: x['market_cap_rank'])
        
        print(f"{exchange_name} 交集结果: {len(intersection_details)} 个币种")
        return intersection_details
    
    def generate_comprehensive_whitelist(self):
        """生成综合白名单"""
        print("开始获取市值数据...")
        market_cap_data = self.fetcher.get_market_cap_rankings(300)
        
        if not market_cap_data:
            print("无法获取市值数据，退出")
            return {}
        
        time.sleep(1)  # API调用间隔
        
        print("\n开始获取各交易所交易量数据...")
        binance_volume = self.fetcher.get_binance_volume_rankings()
        time.sleep(1)
        
        okx_volume = self.fetcher.get_okx_volume_rankings()
        time.sleep(1)
        
        bybit_volume = self.fetcher.get_bybit_volume_rankings()
        
        # 计算各交易所的交集
        print("\n计算交集...")
        binance_intersection = self.get_intersection_pairs(
            market_cap_data, binance_volume, 100, 100, 'binance'
        )
        
        okx_intersection = self.get_intersection_pairs(
            market_cap_data, okx_volume, 100, 100, 'okx'  
        )
        
        bybit_intersection = self.get_intersection_pairs(
            market_cap_data, bybit_volume, 50, 50, 'bybit'
        )
        
        # 合并所有交易对
        all_pairs = set()
        exchange_details = {}
        
        for item in binance_intersection:
            all_pairs.add(item['symbol'])
            exchange_details[item['symbol']] = item
            exchange_details[item['symbol']]['exchanges'] = ['binance']  # 修正：初始化exchanges列表
        
        for item in okx_intersection:
            all_pairs.add(item['symbol'])
            if item['symbol'] in exchange_details:
                exchange_details[item['symbol']]['exchanges'].append('okx')  # 修正：使用方括号
            else:
                exchange_details[item['symbol']] = item
                exchange_details[item['symbol']]['exchanges'] = ['okx']  # 修正：使用方括号
        
        for item in bybit_intersection:
            all_pairs.add(item['symbol'])
            if item['symbol'] in exchange_details:
                exchange_details[item['symbol']]['exchanges'].append('bybit')  # 修正：使用append
            else:
                exchange_details[item['symbol']] = item
                exchange_details[item['symbol']]['exchanges'] = ['bybit']
        
        # 按市值排名排序最终白名单
        final_pairs = []
        for symbol in all_pairs:
            final_pairs.append(exchange_details[symbol])
        
        final_pairs.sort(key=lambda x: x['market_cap_rank'])
        
        # 生成白名单结构
        whitelist = {
            "exchange": {
                "pair_whitelist": [pair['symbol'] for pair in final_pairs]
            },
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_pairs": len(final_pairs),
                "criteria": {
                    "binance": "市值前100 ∩ 交易量前100",
                    "okx": "市值前100 ∩ 交易量前100", 
                    "bybit": "市值前50 ∩ 交易量前50"
                },
                "data_sources": ["CoinGecko", "Binance", "OKX", "Bybit"]
            },
            "exchange_breakdown": {
                "binance_pairs": len(binance_intersection),
                "okx_pairs": len(okx_intersection),
                "bybit_pairs": len(bybit_intersection)
            },
            "pair_details": final_pairs
        }
        
        return whitelist
    
    def save_whitelist(self, filename="intersection_whitelist.json"):
        """保存白名单到文件"""
        whitelist = self.generate_comprehensive_whitelist()
        
        if not whitelist:
            print("无法生成白名单")
            return
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(whitelist, f, indent=2, ensure_ascii=False)
        
        print(f"\n白名单已保存到 {filename}")
        print(f"总交易对数量: {whitelist['metadata']['total_pairs']}")
        print(f"币安交集: {whitelist['exchange_breakdown']['binance_pairs']} 个")
        print(f"OKX交集: {whitelist['exchange_breakdown']['okx_pairs']} 个") 
        print(f"Bybit交集: {whitelist['exchange_breakdown']['bybit_pairs']} 个")
        
        # 显示前15个交易对
        print(f"\n前15个交易对 (按市值排名):")
        for i, pair in enumerate(whitelist['pair_details'][:15], 1):
            exchanges = pair.get('exchanges', [pair['exchange']])
            print(f"{i}. {pair['symbol']} - 市值排名:{pair['market_cap_rank']} "
                  f"交易量排名:{pair['volume_rank']} 交易所:{','.join(exchanges)}")

if __name__ == "__main__":
    generator = IntersectionWhitelistGenerator() 
    generator.save_whitelist()
