# DESC Sprint Week October 2021 - TOM Toolkit and Elasticc challenge

## IN2P3 and NERSC

```bash
# log in to IN2P3
ssh traen@cca.in2p3.fr
# log in to tom_desc vm: http://134.158.244.163:8000/
ssh -i ~/.ssh/id_rsa_nophrase traen@172.17.8.15

# log in to NERSC
ssh troyraen@cori.nersc.gov
# upload key
scp GCP_auth_key-pitt_broker_user_project.json troyraen@cori.nersc.gov:.
# give key to Alex Kim
give -u akim GCP_auth_key-pitt_broker_user_project.json
```

## Docker Compose with Nic Wolf's config

<!-- prereqs

following readme at [https://github.com/edenhill/librdkafka](https://github.com/edenhill/librdkafka)

```bash
# install librdkafka... install homebrew first
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
# follow instructions

# following instructions to adde homebrew to my path
# caused me to loose access to terminal commands like `ls`
# followed this to fix: https://osxdaily.com/2018/10/09/fix-operation-not-permitted-terminal-error-macos/
# then re-install homebrew and add this to path in ~/.zshrc: /opt/homebrew/bin

brew install librdkafka
``` -->

```bash
# git clone ...
# cd ...
git checkout u/nicwolf/development

# start Docker, then
docker-compose up
# then point your browser at localhost:8080

# to rebuild:
docker-compose up --build tom

nano stream/management/commands/pittgoogle.py
nano stream/management/commands/pittgoogle_consumer.py
nano stream/management/commands/utils.py


```

```python
# import os
# from django.core.wsgi import get_wsgi_application
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tom_desc.settings')
# application = get_wsgi_application()
import troy
troy.load_apps()
from stream.management.commands import pittgoogle

cmd = pittgoogle.Command()
cmd.handle()

# debug
from django.conf import settings
from stream.models import Alert, Topic
from stream.management.commands.pittgoogle_consumer import ConsumerStreamPython as Consumer

PITTGOOGLE_CONSUMER_CONFIGURATION = settings.PITTGOOGLE_CONSUMER_CONFIGURATION
consumer = Consumer(PITTGOOGLE_CONSUMER_CONFIGURATION['subscription_name'])
kwargs = {
    # kwargs to be passed to parse_and_save
    # note that in Pub/Sub, the topic and subscription can have different names
    "topic_name": consumer.topic_path.split("/")[-1],
    "send_alert_bytes": True,
    "send_metadata": True,
    # kwargs for stopping conditions
    **PITTGOOGLE_CONSUMER_CONFIGURATION,
}

def parse_and_save(alert_dict, alert_bytes, metadata_dict, **kwargs):
    topic, _ = Topic.objects.get_or_create(name=kwargs["topic_name"])
    alert = Alert.objects.create(
        topic=topic,
        # raw_message=alert_bytes,
        parsed_message=alert_dict,
        # metadata=metadata_dict,
    )
    parser_class = get_parser_class(topic.name)
    with transaction.atomic():
        # Get the parser class, instantiate it, parse the alert, and save it
        parser = parser_class(alert)
        alert.parsed = parser.parse()
        if alert.parsed is True:
            alert.save()
    # TODO: catch errors
    success = True
    return success

itr = consumer.stream_alerts(
    user_callback=parse_and_save,
    **kwargs
)
```


## VM tomtest

```bash
gcloud compute instances create tomtest
gcloud compute instances set-machine-type tomtest --machine-type=f1-micro

gcloud compute scp "/Users/troyraen/Documents/broker/repo/GCP_auth_key-pitt_broker_user_project.json" troyraen@tomtest:GCP_auth_key-pitt_broker_user_project.json
gcloud compute scp "/Users/troyraen/Documents/broker/tommy/tommy/settings.py" troyraen@tomtest:settings.py

sshg tomtest
# conda create --name tomtest python=3.7
# conda activate tomtest
# pip install tom-pittgoogle
# pip install ipython
sudo apt-get update
sudo apt-get install -y python3-pip wget screen software-properties-common
sudo pip3 install ipython

pip3 install tom-pittgoogle

export GOOGLE_CLOUD_PROJECT="pitt-broker-user-project"
export GOOGLE_APPLICATION_CREDENTIALS="/home/troyraen/GCP_auth_key-pitt_broker_user_project.json"
# export SECRET_KEY='4iq)g7qh+1+0g03$!3kx0@*=v!#2ioi@^-f=-^ix6l(z7c_6d8'
export DJANGO_SETTINGS_MODULE='settings'

# update settings file to remove references to tommy
nano settings.py
```

```python
import os
from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()

from tom_pittgoogle.broker_stream_python import BrokerStreamPython

parameters = {
    # pull a small number of alerts from the heartbeat stream for testing
    "subscription_name": "ztf-loop",
    "max_results": 10,
    "timeout": 30,
    "max_backlog": 10,
    # "classtar_threshold": None,
}
broker = BrokerStreamPython()
itr = broker.fetch_alerts(parameters)

```

## Local tom_desc instance

following instructions at [tom_desc](https://github.com/LSSTDESC/tom_desc)

```bash
conda activate tom
cd /Users/troyraen/Documents/broker/tom-desc

export DJANGO_SETTINGS_MODULE="tom_desc.settings"
export GOOGLE_CLOUD_PROJECT="pitt-broker-user-project"
export GOOGLE_APPLICATION_CREDENTIALS="/Users/troyraen/Documents/broker/repo/GCP_auth_key-pitt_broker_user_project.json"

DB_PASS="my4local8secret5"
DB_HOST="localhost"


export DB_HOST=127.0.0.1
docker run --name tom-desc-postgres -v /var/lib/postgresql/data -p 5432:5432 -e POSTGRES_PASSWORD="$DB_PASS" -d postgis/postgis:11-2.5-alpine
docker exec -it tom-desc-postgres /bin/bash  # start a shell inside the postgres container
createdb -U postgres tom_desc                # create the tom_demo database
exit                                         # leave the container, back to your shell

./manage.py migrate           # create the tables
# fails
# need to install gdal
# https://docs.djangoproject.com/en/3.2/ref/contrib/gis/install/geolibs/
./manage.py collectstatic     # gather up the static files for serving

```

## Create Target

```python
import os
from django.core.wsgi import get_wsgi_application
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tommy.settings')
application = get_wsgi_application()
from django.conf import settings
from django.contrib.auth.models import Group
from tom_alerts.alerts import get_service_class
from tom_targets.models import Target
from tom_pittgoogle.consumer_stream_python import ConsumerStreamPython as Consumer

kwargs = {
        # "desc_group": Group.objects.get(name='DESC'),
        "broker": get_service_class('Pitt-Google StreamPython')(),
        # **PITTGOOGLE_CONSUMER_CONFIGURATION,  # stopping conditions
}
alert_dict = {
    "objectId": "myobject8",
    "candid": 2345,
    "ra": 3.42,
    "dec": 34,
    "Bogus": 0.3,
}
target = kwargs["broker"].to_target(alert_dict)
target, created = Target.objects.get_or_create(
    name=alert_dict['objectId'],
    type='SIDEREAL',
    ra=alert_dict['ra'],
    dec=alert_dict['dec'],
)
extra_fields = [item["name"] for item in settings.EXTRA_FIELDS]
extras = {k: v for k, v in alert_dict.items() if k in extra_fields}
target.save(extras=extras)

import pittgoogle
cmmd = pittgoogle.Command()
cmmd.handle()
```

From scratch

```python
import os
from tom_targets.models import Target
from django.core.wsgi import get_wsgi_application
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tommy.settings')
application = get_wsgi_application()

alert = {
    'objectId': 'oid',
    'candid': 1234,

}

def to_target(alert):
    return Target(
        identifier=alert['candid'],
        name=alert['name'],
        type='SIDEREAL',
        designation='MY ALERT',
        ra=alert['ra'],
        dec=alert['dec'],
    )
```
## scratch


```python
GOOGLE_APPLICATION_CREDENTIALS=os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
creds, project = auth.load_credentials_from_file(GOOGLE_APPLICATION_CREDENTIALS)
client = pubsub_v1.SubscriberClient(credentials=creds)
subscription_path = client.subscription_path(project, 'ztf-loop')
request={"subscription": subscription_path, "max_messages": 1}
response = client.pull(request)
```
