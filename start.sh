cd /home/drew/work/ctf/level4
./build.sh
rm -rf /tmp/sqlcluster
./sqlcluster -v -d /tmp/sqlcluster/node0 &
sleep 2
./sqlcluster -v -d /tmp/sqlcluster/node1 -l 127.0.0.1:4001 --join 127.0.0.1:4000 &
sleep 2
./sqlcluster -v -d /tmp/sqlcluster/node2 -l 127.0.0.1:4002 --join 127.0.0.1:4000 &
