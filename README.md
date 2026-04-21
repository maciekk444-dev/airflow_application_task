Instalation steps [Linux]:
=========================

1. Install Docker on your machine:

https://docs.docker.com/engine/install/

2. Install Astro CLI:

https://www.astronomer.io/docs/astro/cli/install-cli

3. Clone this repository in a folder

https://github.com/maciekk444-dev/airflow_application_task

4. Init project in a folder 

astro dev init

5. Start project

astro dev start

6. Access airflow via localhost address in cli. 

Execution of scripts should be automated. 

Files are writing inside Docker container running airflow. To access them check 

astro dev bash


To send emails you need to set .env file with these variables:
AIRFLOW__CORE__TEST_CONNECTION
AIRFLOW__EMAIL__EMAIL_BACKEND
AIRFLOW__SMTP__SMTP_HOST
AIRFLOW__SMTP__START_TLS
AIRFLOW__SMTP__SMTP_SSL
AIRFLOW__SMTP__SMTP_USER
AIRFLOW__SMTP__SMTP_PASSWORD
AIRFLOW__SMTP__SMTP_PORT
AIRFLOW__SMTP__SMTP_MAIL_FROM

and replace test@gmail.com in dags.
