# Install Jupyter with Python 3

If you have Python 3 installed or you do not need to use Docker, you could skip following section.

### Create Ubuntu Docker Container

```bash
$ docker run -ti --name ubu-jupyter --network host ubuntu bash
root@linuxkit-00155d8d4b0f:/#
```

#### Setup a Jupyter User

```bash
$ groupadd jupyter
$ useradd -g jupyter -md /home/jupyter jupyter
$ exit
```

The last command will terminate the container.

```bash
docker start ubu-jupyter
docker exec --user jupyter -ti ubu-jupyter bash
```

#### Install Python3 and pip3

The Ubuntu docker image comes with minimal set of packages pre-installed. Let's first install Python 3.

```bash

# Inside the container
# Since you are root by default there is no need to sudo
$ apt-get update
$ apt-get upgrade
$ apt-get install -y vim
$ apt-get install -y nano
$ apt-get install -y curl
# To see what python3 options are available:
$ apt-cache search python3 | grep 'python3 -'
# To install Python3
$ apt-get install -y python3
# To install pip3
# Takes a while and adds 350 MB
$ apt-get install -y python3-pip

```

### Install Jupyter with Pip

```bash
$ pip3 install jupyter
```

```bash
$ jupyter notebook --generate-config
Writing default config to: /home/jupyter/.jupyter/jupyter_notebook_config.py
```

Update config

```text
c.NotebookApp.allow_remote_access=True
c.NotebookApp.ip = '0.0.0.0'
```

### Resources

* Automate the Boring Stuff with Python \[book\] - [Appendix A - Installing Third Party Modules](https://automatetheboringstuff.com/appendixa/)
* [http://www.jupyter.com](http://www.jupyter.com)

