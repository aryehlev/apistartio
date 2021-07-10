# apistartio

## steps to get up and running:

git clone https://github.com/aryehlev/apistartio.git

cd apistartio

virtualenv env -p 3.8 (works with >3.7) 

env\Scripts\activate - windows,  source env/bin/activate - mac/linux

pip install -r requierments.txt

uvicorn main:app --reload


## next thing todo:
1:implements caching

2: deploying (prob via docker, includes setting up https by getting SSL certificate and setting up dns and uploading to server/servers/AWS/..)

3: setting up more sevices, to get more data(maybe in bulks)/get different data and so on

4:more testing(database and so on)
