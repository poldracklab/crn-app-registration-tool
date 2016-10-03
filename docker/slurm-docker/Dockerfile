FROM agaveapi/centos-base:latest

# Add slurm user
RUN adduser slurm && \
    echo "slurm:slurm" | chpasswd
USER slurm
RUN mkdir /home/slurm/.ssh
COPY ssh/id_rsa.pub /home/slurm/.ssh/authorized_keys
USER root

# Install slurm
RUN yum -y install gcc gcc-g++ make munge munge-devel httpd bzip2 vim-minimal tar perl git

# Configure munge
RUN create-munge-key

# Install slurm
RUN curl -ssL https://github.com/SchedMD/slurm/archive/slurm-14-03-9-1.tar.gz | tar -xzC /usr/local/ && \
    mv /usr/local/slurm-slurm-14-03-9-1 /usr/local/slurm
WORKDIR /usr/local/slurm
RUN ./configure --prefix=/usr --sysconfdir=/etc/sysconfig/slurm --with-mysql_config=/usr/local/bin && \
    make && \
    make install && \
    mkdir -p /etc/sysconfig/slurm && \
    cp etc/init.d.slurm /etc/init.d/slurmd && \
    chmod +x /etc/init.d/slurmd && \
    cp -rf doc/html /var/www/html/slurm && \
    chown -R apache:apache /var/www/html/slurm && \
    chown -R root:root /var/log/munge && \
    chown -R root:root /var/lib/munge && \
    mkdir /var/run/munge && \
    chown -R root:root /var/run/munge && \
    chown -R root:root /etc/munge && \
    mkdir /var/log/slurm && \
    touch /var/log/slurm/job_completions && \
    touch /var/log/slurm/accounting && \
    chown -R slurm:slurm /var/log/slurm && \
    touch /var/spool/last_config_lite && \
    touch /var/spool/last_config_lite.new && \
    chown slurm:slurm /var/spool/last_config_lite* && \
    chown root:slurm /var/spool && \
    chmod g+w /var/spool

COPY slurm/slurm.conf /etc/sysconfig/slurm/slurm.conf

RUN ENCPASS=$( echo "testpass" | openssl passwd -1 -stdin); \
    useradd -p $ENCPASS -U -m -G users,testuser -u 1000 -s /bin/bash circleci

USER circleci
COPY slurm/slurm-child.submit /home/circleci/slurm-child.submit
COPY slurm/slurm-parent.submit /home/circleci/slurm-parent.submit

USER root
RUN chown circleci:circleci /home/circleci/slurm-child.submit
RUN chown circleci:circleci /home/circleci/slurm-parent.submit
COPY supervisord.conf /etc/supervisord.conf
RUN mkdir /var/log/supervisor
RUN chmod -R 777 /var/log/supervisor

COPY testapp.sh /usr/bin/testapp
RUN chmod +x /usr/bin/testapp

EXPOSE 10389 22 6817 6818
CMD /usr/bin/supervisord
