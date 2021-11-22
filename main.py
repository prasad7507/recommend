from flask import Flask, json, jsonify, request
from werkzeug.wrappers import response
import pandas as pd
from google.cloud import storage
import pandas as pd
import io
from io import BytesIO
from math import sqrt
app = Flask(__name__)


class ServerRequest:
    def __init__(self):
        global storage_clinet, bucket_name, ll, user_order_dataframe
        storage_clinet = storage.Client.from_service_account_json(
            'connection-f81bb-firebase-adminsdk-662co-2adb086890.json')
        bucket_name = 'connection-f81bb.appspot.com'
        bucket = storage_clinet.get_bucket(bucket_name)
        ll = pd.read_csv(io.BytesIO(bucket.blob(
            blob_name='productId.csv').download_as_string()), encoding='UTF-8', sep=',')
        user_order_dataframe = pd.read_csv(io.BytesIO(bucket.blob(
            blob_name='user_orders.csv').download_as_string()), encoding='UTF-8', sep=',')


obj = ServerRequest()


@app.route('/', methods=["POST", "GET"])
def index():
    try:
        if request.method == "POST":
            data = request.data
            data = json.loads(data)
            data = data['id']
            userInput = []
            for i in range(0, len(data)):
                userInput.append({'productId': data[i], 'rating': 0})
            products = pd.DataFrame(userInput)
            userSubsetGroup = user_order_dataframe[user_order_dataframe.productId.isin(
                products['productId'])]
            userSubsetGroup = user_order_dataframe.groupby(['userId'])
            userSubsetGroup = sorted(
                userSubsetGroup,  key=lambda x: len(x[1]), reverse=True)
            pearsonCorrelationDict = {}
            for name, group in userSubsetGroup:
                group = group.sort_values(by='productId')
                products = products.sort_values(by='productId')
                nRatings = len(group)
                temp_df = group[group['productId'].isin(
                    products['productId'].tolist())]
                tempRatingList = temp_df['rating'].tolist()
                tempGroupList = group['rating'].tolist()
                Sxx = sum([i**2 for i in tempRatingList]) - \
                    pow(sum(tempRatingList), 2)/float(nRatings)
                Syy = sum([i**2 for i in tempGroupList]) - \
                    pow(sum(tempGroupList), 2)/float(nRatings)
                Sxy = sum(i*j for i, j in zip(tempRatingList, tempGroupList)) - \
                    sum(tempRatingList)*sum(tempGroupList)/float(nRatings)
                if Sxx != 0 and Syy != 0:
                    pearsonCorrelationDict[name] = Sxy/sqrt(Sxx*Syy)
                else:
                    pearsonCorrelationDict[name] = 0
            pearsonDF = pd.DataFrame.from_dict(
                pearsonCorrelationDict, orient='index')
            pearsonDF.columns = ['similarityIndex']
            pearsonDF['userId'] = pearsonDF.index
            pearsonDF.index = range(len(pearsonDF))
            topUsers = pearsonDF.sort_values(
                by='similarityIndex', ascending=False)[0:50]
            topUsersRating = topUsers.merge(
                user_order_dataframe, left_on='userId', right_on='userId', how='inner')
            topUsersRating['weightedRating'] = topUsersRating['similarityIndex'] * \
                topUsersRating['rating']
            tempTopUsersRating = topUsersRating.groupby(
                'productId').sum()[['similarityIndex', 'weightedRating']]
            tempTopUsersRating.columns = [
                'sum_similarityIndex', 'sum_weightedRating']
            recommendation_df = pd.DataFrame()
            recommendation_df['weighted average recommendation score'] = tempTopUsersRating['sum_weightedRating'] / \
                tempTopUsersRating['sum_similarityIndex']
            recommendation_df['productId'] = tempTopUsersRating.index
            recommendation_df = recommendation_df.sort_values(
                by='weighted average recommendation score', ascending=False)
            l = pd.DataFrame(ll)
            l.rename(columns={0: 'productId'}, inplace=True)
            rec_products = l.loc[l['productId'].isin(
                recommendation_df.head(50)['productId'].tolist())]
            rec_products.dropna()
            data = rec_products['productId'].to_list()
            if len(data) > 10:
                data = data[:10]
            else:
                data = data[:len(data)]
            return jsonify(id=data)
        elif request.method == "GET":
            return "Welcome to Product Recommendation Project"
        else:
            return "Invalid Request"
    except:
        return "Something Went Wrong"


if __name__ == '__main__':
    app.run(debug=True)
