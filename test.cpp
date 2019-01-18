#include <unordered_map>
#include <unordered_set>
#include <set>
#include <string>
#include <vector>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <ctime>
#include <thread>
#include <mutex>
#include <chrono>

using namespace std;

int num_tables = 10;
vector<string> tables;

int nchoosek(int n, int k)
{
    int ans = 1;

    for (int i = 0; i < k; i++)
    {
        ans *= (n - i);
    }

    for (int i = 0; i < k; i++)
    {
        ans /= (i + 1);
    }

    return ans;
}

string get_comb_str(int size, vector<string> tables, int index) /**index start from 1*/
{
    string res;

    int sum_tmp = 0, dk = 1, dn = 0;

    for (int i = 0; i < size; i++)
    {
        int add_tmp = 0;

        while (sum_tmp < index) //todo: <= ?
        {
            add_tmp = nchoosek(num_tables - (++dn), size - dk); //todo: optimize

            sum_tmp += add_tmp;
        }

        if (sum_tmp == index)
        {
            dn++;
        } else {
            sum_tmp -= add_tmp;
        }

//        if (size == 1)
//        {
//            dn--;
//        }
        res.append(tables[dn - 1]);

        if (i < size - 1)
        {
            res.append("-");
        }

        dk++;
    }

    return res;
}

int main(int argc, char* argv[]) 
{
    for (int tbl_idx = 1; tbl_idx <= num_tables; tbl_idx++)
    {
        char ch = 'a' - 1 + tbl_idx;

        stringstream ss_;
        string s_;

        ss_ << ch;
        ss_ >> s_;

        //cout << s_ << endl;

        tables.push_back(s_);
    }

    for (int i = 1; i < 10; i++) {
        string res = get_comb_str(i, tables, 2);
        cout << res << endl;
    }

}
