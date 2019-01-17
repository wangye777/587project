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

/** parameter **/
bool debug = true;
int num_tables = 12;
int num_threads = 4;
string graphtopo;

bool multithread = true;

/** global variables **/
unordered_map<string, double> cost_dict; // (a-c-d) -> 0.875 using sorted order, record the minimum cost only
mutex mu_cd;
unordered_map<string, string> helper; // (a-c-d) -> (c-a-d) this is the original order
mutex mu_h;
unordered_map<int, set<string> > memo; // 3-> set{(a-c-d), (a-b-d)...} using sorted order
mutex mu_memo;
unordered_map<string, vector<int> > skip_vector; //skip vector map: (a-c-d) -> [3, 8, 5, 4] = [id, [SV]]
mutex mu_sv;

bool intersection(string qep1, string qep2);
int intersectionSV(string qep1, string qep2);
double get_pred_cost(string qep);
string qep_norm(string qep);
void multi_plan_join(int size, int thread_id);
void skip_vector_build(int size, int thread_id);
void test(int val, int id);

int main(int argc, char* argv[]) {

    if (argc == 0)
    {
        //default: num_tables = 12; num_threads = 4; graphtopo = "Clique"
        graphtopo = "Star";
    }

    if (argc == 4)
    {
        num_tables = stoi(argv[1]);

        num_threads = stoi(argv[2]);

        graphtopo = argv[3];
    }

    //assert num of tables > 1
    assert(num_tables > 1);

    vector<string> tables;

    if (num_threads == 1)
    {
        multithread = false;
    }

    for (int tbl_idx = 1; tbl_idx <= num_tables; tbl_idx++)
    {
        char ch = (char)('a' - 1 + tbl_idx);

        stringstream ss_;
        string s_;

        ss_ << ch;
        ss_ >> s_;

        //cout << s_ << endl;

        tables.push_back(s_);
    }

    //initialize memo
    int size = 1;

    set<string> qeps;

    /** time start, clock() is all cpu time **/
    clock_t begin = clock();
    auto wall_start = chrono::steady_clock::now();

    for (int i = 0; i < num_tables; i++)
    {
        qeps.insert(tables[i]);

        cost_dict.insert(make_pair(tables[i], get_pred_cost(tables[i])));

        //todo: initialize size = 1 quantifier skip vector !
        skip_vector.insert(make_pair(tables[i], vector<int>{i, i + 1}));
    }

    memo.insert(make_pair(size, qeps));

    thread threads_gp1[num_threads * (num_tables - 1)];
    thread threads_gp2[num_threads * (num_tables - 1)];

    int thread_id = 0;

    // iteration of size from 2 to num_tables
    for (size = 2; size <= num_tables; size++)
    {
        /** allocate search space **/

        if (!multithread)
        {
            multi_plan_join(size, 0);

            skip_vector_build(size, 0);

        } else {

            for (thread_id = 0; thread_id < num_threads; thread_id++)
            {
                threads_gp1[thread_id + num_threads * (size - 2)] = thread(multi_plan_join, size, thread_id);
            }

            for (int th_id = num_threads * (size - 2); th_id < num_threads * (size - 1); th_id++)
            {
                if (threads_gp1[th_id].joinable())
                {
                    threads_gp1[th_id].join();
                }
            }

            for (thread_id = 0; thread_id < num_threads; thread_id++)
            {
                threads_gp2[thread_id + num_threads * (size - 2)] = thread(skip_vector_build, size, thread_id);
            }

            for (int th_id = num_threads * (size - 2); th_id < num_threads * (size - 1); th_id++)
            {
                if (threads_gp2[th_id].joinable())
                {
                    threads_gp2[th_id].join();
                }
            }
        }


        if (graphtopo == "Clique")
        {
            //do nothing
        } else {
            set<string> query = memo.at(2);

            set<string> :: iterator query_itr;

            for (query_itr = query.begin(); query_itr != query.end(); query_itr++)
            {
                string q = *query_itr;

                if (graphtopo == "Star")
                {
                    if (q.at(0) != 'a')
                    {
                        query.erase(query_itr);
                    }
                }

                if (graphtopo == "Linear")
                {
                    if (q.at(2) - q.at(0) != 1)
                    {
                        query.erase(query_itr);
                    }
                }
            }
        }
    }

    /** time stop **/
    clock_t end = clock();
    auto wall_end = chrono::steady_clock::now();

    double elapsed_secs = double(end - begin) / CLOCKS_PER_SEC;
    double elapsed_wall = (double)chrono::duration_cast<chrono::milliseconds>(wall_end - wall_start).count() / 1000;

    string final_plan = helper[*memo[size - 1].begin()];

    double final_cost = cost_dict[*memo[size - 1].begin()];

    cout << "the final plan: " << final_plan << "  cost: " << final_cost << endl;
    cout << "All cpu time: " << elapsed_secs << endl;
    cout << "Wall time: " << elapsed_wall << endl;
}

bool intersection(string qep1, string qep2)
{
    //split qep1 into a set of strings, the compare with qep2;
    stringstream ss1(qep1);

    string item;

    unordered_set<string> small_set;

    while (getline(ss1, item, '-'))
    {
        small_set.insert(item);
    }

    stringstream ss2(qep2);

    auto const not_found = small_set.end();

    while (getline(ss2, item, '-'))
    {
        if (small_set.find(item) != not_found)
        {
            return true;
        }
    }

    return false;
}

int intersectionSV(string qep1, string qep2)
{
    //split qep1 into a set of strings, the compare with qep2;
    stringstream ss1(qep1);

    string item;

    unordered_set<string> small_set;

    while (getline(ss1, item, '-'))
    {
        small_set.insert(item);
    }

    stringstream ss2(qep2);

    auto const not_found = small_set.end();

    if (skip_vector.find(qep2) == skip_vector.end())
    {
        cout << qep2 << " : No Skip Vector ! !" << endl;
    }
    vector<int> sv = skip_vector.at(qep2);

    int dist = sv[0];

    int dist_ptr = 0;

    while (getline(ss2, item, '-'))
    {
        dist_ptr++;

        if (small_set.find(item) != not_found)
        {
            dist = max(dist, sv[dist_ptr]);
        }
    }

    return dist - sv[0];
}

double get_pred_cost(string qep)
{
    return 1.0;
}

string qep_norm(string qep)
{
    stringstream ss(qep);

    string item;

    vector<string> vec_tbl;

    while (getline(ss, item, '-'))
    {
        vec_tbl.push_back(item);
    }

    // comparator: <
    sort(vec_tbl.begin(), vec_tbl.end());

    string norm_qep;

    int flag = 0;

    for (auto val : vec_tbl)
    {
        if (flag != 0)
        {
            norm_qep.append("-");
        }

        norm_qep.append(val);

        flag++;
    }

    return norm_qep;
}

void multi_plan_join(int size, int thread_id)
{
    //initialize iterator for smallSZ and largeSZ
    set<string> :: iterator smallQS_itr;
    set<string> :: iterator largeQS_itr;

    int inner_idx;
    bool mu_cd_flag;

    /**for test **/
    int p_l, p_r;
    int cal = 0;

    for (int small_size = 1; small_size <= size / 2; small_size++) {

        //calculate the size of larger set
        int large_size = size - small_size;

        //get smaller set of qep
        set<string> smallQS = memo.at(small_size);

        //get larger set of qep
        set<string> largeQS = memo.at(large_size);

        int size_largeQS = (int) largeQS.size();

        int partition_left = thread_id * (size_largeQS / num_threads);

        int partition_right = thread_id == num_threads - 1 ? size_largeQS - 1 : (thread_id + 1) * (size_largeQS / num_threads);

        //p_l = partition_left;
        //p_r = partition_right;

        for (smallQS_itr = smallQS.begin(); smallQS_itr != smallQS.end(); smallQS_itr++)
        {
            inner_idx = 0;

            int skip_num = 0;

            for (largeQS_itr = largeQS.begin(); largeQS_itr != largeQS.end(); largeQS_itr++)
            {
                if (inner_idx < partition_left)
                {
                    inner_idx++;
                    continue;
                }

                if (inner_idx > partition_right)
                {
                    break;
                }

                if (skip_num > 0)
                {
                    skip_num--;
                    inner_idx++;
                    continue;
                }

                inner_idx++;

                //get the string format of qep
                string qep_smallQS = *smallQS_itr;
                string qep_largeQS = *largeQS_itr;

                // if the intersection of tables involved is not empty, continue
                skip_num = intersectionSV(qep_smallQS, qep_largeQS);

                if (skip_num > 0)
                {
                    skip_num--;
                    continue;
                }

                //create new qep, append to the small one
                qep_smallQS.append("-").append(qep_largeQS);

                double new_qep_cost = get_pred_cost(qep_smallQS);

                string norm_new_qep = qep_norm(qep_smallQS);

                /** for test**/
                cal++;

                mu_cd.lock();

                mu_cd_flag = false;

                if (cost_dict.find(norm_new_qep) == cost_dict.end())
                {
                    cost_dict.insert(make_pair(norm_new_qep, new_qep_cost));
                    mu_cd.unlock();

                    mu_cd_flag = true;

                    mu_h.lock();
                    helper.insert(make_pair(norm_new_qep, qep_smallQS));
                    mu_h.unlock();

                    mu_memo.lock();
                    if (memo.find(size) == memo.end())
                    {
                        set<string> tmp_qep_set;

                        tmp_qep_set.insert(norm_new_qep);

                        memo.insert(make_pair(size, tmp_qep_set));
                        mu_memo.unlock();

                    } else {

                        memo[size].insert(norm_new_qep);
                        mu_memo.unlock();
                    }

                } else if (cost_dict[norm_new_qep] > new_qep_cost) {

                    cost_dict[norm_new_qep] = new_qep_cost;
                    mu_cd.unlock();

                    mu_cd_flag = true;

                    mu_h.lock();
                    helper[norm_new_qep] = qep_smallQS;
                    mu_h.unlock();
                }

                if (!mu_cd_flag)
                {
                    mu_cd.unlock();
                }
            }
        }
    }

    if (debug)
    {
        cout << "thread:" << thread_id << " finished for size: " << size << " compute: " << cal << endl;
    }
}

void skip_vector_build(int size, int thread_id)
{
    mu_memo.lock();
    set<string> qtfr_set = memo.at(size);
    mu_memo.unlock();

    int new_qtfr_size = (int)qtfr_set.size();

    int left_boundary = thread_id * (new_qtfr_size / num_threads);

    int right_boundary = (thread_id == num_threads - 1) ? new_qtfr_size - 1 : (thread_id + 1) * (new_qtfr_size / num_threads) - 1;

    int r_counter = new_qtfr_size - 1;

    //initialize last_skip_vector
    string last_s = "*";

    vector<int> last_sv((unsigned long)(size + 1), right_boundary + 1);

    last_sv[0] = right_boundary;

    set<string> :: reverse_iterator ritr;

    for (ritr = qtfr_set.rbegin(); ritr != qtfr_set.rend(); ritr++)
    {
        if (r_counter > right_boundary)
        {
            r_counter--;
            continue;
        }

        if (r_counter < left_boundary)
        {
            break;
        }

        //cout << *ritr << endl;
        if (skip_vector.find(*ritr) == skip_vector.end()) //no need to check?
        {
            if (last_s != "*")
            {
                string now_s = *ritr;

                last_sv[0] = r_counter;

                vector<int> last_sv_copy = last_sv;

                unsigned long last_s_iter = 0;

                for (unsigned long now_s_iter = 0; now_s_iter < size; now_s_iter++)
                {
                    if (now_s.at(2 * now_s_iter) == last_s.at(2 * last_s_iter))
                    {
                        last_sv[now_s_iter + 1] = last_sv_copy[last_s_iter + 1];

                    } else if (now_s.at(2 * now_s_iter) < last_s.at(2 * last_s_iter))
                    {
                        last_sv[now_s_iter + 1] = r_counter + 1;

                    } else
                    {
                        last_s_iter++;

                        if (last_s_iter == size)
                        {
                            while (now_s_iter < size)
                            {
                                last_sv[now_s_iter + 1] = r_counter + 1;

                                now_s_iter++;
                            }

                            break;
                        }

                        now_s_iter--;
                    }
                }
            }

            mu_sv.lock();
            skip_vector.insert(make_pair(*ritr, last_sv));
            mu_sv.unlock();

            last_s = *ritr;
        }

        r_counter--;
    }
}

void test(int val, int id)
{
    cout << val << " " << id << endl;
}