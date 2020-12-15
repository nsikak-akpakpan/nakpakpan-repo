sudo yum -y update

sudo amazon-linux-extras | grep postgre

sudo tee /etc/yum.repos.d/pgdg.repo<<EOF
[pgdg12]
name=PostgreSQL 12 for RHEL/CentOS 7 - x86_64
baseurl=https://download.postgresql.org/pub/repos/yum/12/redhat/rhel-7-x86_64
enabled=1
gpgcheck=0
EOF

sudo yum makecache

sudo yum install postgresql12 postgresql12-server

$ sudo /usr/pgsql-12/bin/postgresql-12-setup initdb

sudo systemctl enable --now postgresql-12

systemctl status postgresql-12

sudo su - postgres 

  -bash-4.2$ psql -c "alter user postgres with password 'StrongPassword'"
