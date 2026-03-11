# BGD_zadanie1

docker compose -f generate_data_docker.yml build 

docker compose -f generate_data_docker.yml up


docker compose -f pipeline_docker.yml build 

docker compose -f pipeline_docker.yml up


docker compose -f show_data.yml build 

docker compose -f show_data.yml up
