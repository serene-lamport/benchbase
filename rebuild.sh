set -e

# compile stuff
./mvnw package -P postgres -DskipTests

# unpack postgres target
pushd target
echo Extract benchbase locally...
tar xvf benchbase-postgres.tgz

# also install on remote...
echo Copying bencbhase to tembo...
scp benchbase-postgres.tgz tembo:~/PG_TESTS/benchbase_src/target/
echo Extracting benchbase on tebmo...
ssh tembo ". env_extra && cd postgresql-test-scripts && ./run_util.py bbase_reinstall"

popd