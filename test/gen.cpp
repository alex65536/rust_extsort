#include <iostream>
#include <random>

using namespace std;

int main() {
    ios_base::sync_with_stdio(false);
    int sumLen = 0;
    const int maxLen = 200'000'000;
    mt19937 rnd(42);
    geometric_distribution<> distr(0.01);
    while (sumLen < maxLen) {
        int addLen = min(maxLen - sumLen, distr(rnd));
        if (addLen < 2) addLen = 2;
        string str(addLen, '?');
        for (char &c : str) {
            c = 'a' + rnd() % 26;
        }
        cout << str << "\n";
        sumLen += addLen;
    }
    return 0;

}
