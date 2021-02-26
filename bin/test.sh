source ~/.jabba/jabba.sh 
jabba use openjdk@1.10.0
JAVA_TOOL_OPTIONS="-Duser.country=PL -Duser.language=pl -Duser.timezone=UTC" mvn clean install

