# Mqttmsg
- message 형식에 따라 원하는 값을 넣는게 가능
- 어떤 payload가 오는지에 따라 코드는 수정해야함


## Getting started 1
- cd mqttmsg/
- conf/config파일 수정
- sudo python3 msgtocsv.py 실행
- ctrl + c 로 종료


## Getting started 2
- sudo nohup python3 msgtocsv.py &
- sudo pkill -9 -ef msgtocsv.py 로 종료