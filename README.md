# fastApi-start.io

## steps to get up and running:

cd .../fastApi-start.io

virtualenv env -p 3.8 (checked with python 3.8 i think works also with >3.7) need to check still

env\Scripts\activate - windows,  source env/bin/activate - mac/linux

pip install -r requierments.txt

uvicorn main:app --reload


## next thing todo:
1: change to SQL database for processing larger data

2: testing + linting + documenting + CI workflow

3: deploying (prob via docker, includes setting up https by getting SSL certificate and setting up dns and uploading to server/servers/AWS/..)

4: setting up more sevices, to get more data(maybe in bulks)/get different data and so on
