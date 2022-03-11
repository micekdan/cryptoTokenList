import http.client
import json
import csv


def retrieve_tokens(uri, path, queryColumns):
    result = []
    conn = http.client.HTTPSConnection(uri)
    headers = {
        'Content-Type': 'application/json'
    }
    lastAmount = -1
    lastId = ""
    while lastAmount == -1 or lastAmount > 0:
        if(lastAmount != -1):
            condition = "where: {id_gt:\\\""+lastId+"\\\"},"
        else:
            condition = ""
        payload = "{\"query\":\"{tokens(orderBy:id,orderDirection:asc,"+condition+"first:1000){"+",".join(queryColumns)+"}}\",\"variables\":{}}"

        conn.request("POST", path, payload, headers)
        res = conn.getresponse()
        data = res.read()
        decodedData = json.loads(data.decode("utf-8"))
        tokens = decodedData['data']['tokens']
        lastAmount = len(tokens)
        print("lastAmount:" + str(lastId) + ", tokens:" + str(lastAmount))
        result += tokens
        print("resultLength:" + str(len(result)))
        if(lastAmount > 0):
            lastId = tokens[len(tokens)-1]['id']
    return result

def write_to_csv(file, queryColumns, content):
    f = open(file, 'w', encoding='utf-8', newline='')
    writer = csv.writer(f,quoting=csv.QUOTE_NONNUMERIC)
    writer.writerow(queryColumns)
    for record in content:
        row = []
        for column in queryColumns:
            row += [record[column]]
        writer.writerow(row)


class Graph:
    def __init__(self, uri, path, file, queryColumns):
        self.uri = uri
        self.path = path
        self.file = file
        self.queryColumns = queryColumns

uniswap = Graph(
    "api.thegraph.com",
    "/subgraphs/name/uniswap/uniswap-v2",
    "uniswaptokens.csv",
    ['id','symbol','name','decimals','totalSupply','tradeVolume','tradeVolumeUSD','untrackedVolumeUSD','txCount','totalLiquidity','derivedETH'])
    
uniswapTokens = retrieve_tokens(uniswap.uri,uniswap.path,uniswap.queryColumns)
write_to_csv(uniswap.file,uniswap.queryColumns,uniswapTokens)

sushiswap = Graph(
    "api.thegraph.com",
    "/subgraphs/name/sushiswap/exchange",
    "sushiswaptokens.csv",
    ["id","symbol","name","decimals","totalSupply","volume","volumeUSD","untrackedVolumeUSD","txCount","liquidity","derivedETH",])

sushiswapTokens = retrieve_tokens(sushiswap.uri,sushiswap.path,sushiswap.queryColumns)
write_to_csv(sushiswap.file,sushiswap.queryColumns,sushiswapTokens)