// sort.cpp
// Generate N random records (key length 33, value length 600), store in vector<Record>, sort by key and measure time.
// Usage: sort_test N [runs] [seed]

#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <vector>

using namespace std;

struct Record {
	string key;   // 33 bytes
	string value; // 600 bytes
};

static string gen_random_hex(size_t len, std::mt19937_64 &rng) {
	static const char charset[] = "0123456789abcdef"; // 16 chars
	std::uniform_int_distribution<int> dist(0, 15);
	string s;
	s.resize(len);
	for (size_t i = 0; i < len; ++i) s[i] = charset[dist(rng)];
	return s;
}

int main(int argc, char **argv) {
	if (argc < 2) {
		cerr << "Usage: " << argv[0] << " N [runs] [seed]\n";
		return 1;
	}

	size_t N = 0;
	try { N = stoull(argv[1]); } catch (...) { cerr << "Invalid N\n"; return 1; }

	int runs = 1;
	if (argc >= 3) { try { runs = stoi(argv[2]); } catch (...) { runs = 1; } if (runs < 1) runs = 1; }

	unsigned long long seed = (unsigned long long)chrono::high_resolution_clock::now().time_since_epoch().count();
	if (argc >= 4) { try { seed = stoull(argv[3]); } catch (...) { } }

	cout << "N=" << N << " runs=" << runs << " seed=" << seed << "\n";

	const size_t KEY_LEN = 33;
	const size_t VAL_LEN = 600;

	vector<double> times;
	times.reserve(runs);

	for (int r = 0; r < runs; ++r) {
		std::mt19937_64 rng(seed + (unsigned long long)r);

		vector<Record> v;
		v.reserve(N);

		for (size_t i = 0; i < N; ++i) {
			Record rec;
			rec.key = gen_random_hex(KEY_LEN, rng);
			rec.value = gen_random_hex(VAL_LEN, rng);
			v.emplace_back(std::move(rec));
		}

		auto t0 = chrono::steady_clock::now();
		sort(v.begin(), v.end(), [](const Record &a, const Record &b) { return a.key < b.key; });
		auto t1 = chrono::steady_clock::now();

		double ms = chrono::duration_cast<chrono::duration<double, std::milli>>(t1 - t0).count();
		times.push_back(ms);

		cout << "run " << (r + 1) << ": sort time = " << ms << " ms\n";
		if (!v.empty()) {
			cout << "first_key=" << v.front().key.substr(0,6) << " last_key=" << v.back().key.substr(0,6) << "\n";
		}
	}

	double sum = 0.0;
	for (double t : times) sum += t;
	double avg = sum / times.size();
	cout << "average sort time = " << avg << " ms\n";

	return 0;
}

