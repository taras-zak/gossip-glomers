maelstrom:
	brew install openjdk graphviz gnuplot
	wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.4/maelstrom.tar.bz2
	tar -xvf maelstrom.tar.bz2
	rm maelstrom.tar.bz2

run_echo:
	go build -o ./bin/echo ./challenge_1_echo
	./maelstrom/maelstrom test -w echo --bin ./bin/echo --node-count 1 --time-limit 1

run_id_generator:
	go build -o ./bin/id_generator ./challenge_2_unique_id_generator
	./maelstrom/maelstrom test -w unique-ids --bin ./bin/id_generator --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

run_broadcast_a:
	go build -o ./bin/broadcast_a ./challenge_3a_broadcast
	./maelstrom/maelstrom test -w broadcast --bin ./bin/broadcast_a --time-limit 20 --rate 10 --node-count 1

run_broadcast_b:
	go build -o ./bin/broadcast_b ./challenge_3b_broadcast
	./maelstrom/maelstrom test -w broadcast --bin ./bin/broadcast_b --time-limit 20 --rate 10 --node-count 5

debug:
	./maelstrom/maelstrom serve