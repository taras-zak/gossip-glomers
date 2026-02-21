maelstrom:
	brew install openjdk graphviz gnuplot
	wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.4/maelstrom.tar.bz2
	tar -xvf maelstrom.tar.bz2
	rm maelstrom.tar.bz2

build:
	go build -o ./bin/echo ./challenge_1_echo
	go build -o ./bin/id_generator ./challenge_2_unique_id_generator

run_echo:
	./maelstrom/maelstrom test -w echo --bin ./bin/echo --node-count 1 --time-limit 1

run_id_generator:
	./maelstrom/maelstrom test -w unique-ids --bin ./bin/id_generator --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

debug:
	./maelstrom/maelstrom serve