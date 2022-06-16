import pandas as pd

filename = "app/ressources/twitter_combined.csv"

following = {
}

with open(filename) as file:
    for row in file:
        row = row.replace("\n","").split(" ")
        old_val = following.get(row[0])
        if old_val:
            old_val.append(row[1])
            following[row[0]] = old_val
        else:
            following[row[0]] = [row[1]]

followers = {
}

with open(filename) as file:
    for row in file:
        row = row.replace("\n","").split(" ")
        old_val = followers.get(row[1])
        if old_val:
            old_val.append(row[0])
            followers[row[1]] = old_val
            #print(followers[row[1]])
        else:
            followers[row[1]] = [row[0]]

df = pd.DataFrame(columns=["user_id","followers_id","following_id"])
for user in followers:
    following_user = following.get(user)
    df2 = pd.DataFrame([[user,followers[user],following_user]],columns=["user_id","followers_id","following_id"])
    df = pd.concat([df,df2])

json_file = df.to_json(orient='records')

import json
with open('app/ressources/data.json', 'a') as f:
    pass
    f.write(json_file + '\n')

with open('app/ressources/data.json', 'r') as f:
    data = json.load(f)
    for ele in data:
        print(ele)
