#include <iostream> 

using namespace std;

int main()

{
    freopen("TEST_INPUT", "w", stdout);
    int len = 30000;
    cout << "insert into text_table values(1, '";

    for(int i = 0 ; i < len; i++){
        cout << (char)('A'+(i%26));
    }
    cout << "');";
    cout << endl << "exit" << endl;

    return 0;
}