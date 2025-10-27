// RegionDataLoading.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <set>

#include <pqxx/pqxx>

using namespace std;

const string DATA_DIRECTORY = "--data_directory";
const string POINTS_FILE = "points.txt";
const string CATEGORIES_FILE = "categories.txt";
const string GROUPS_FILE = "groups.txt";

const string postgresHost = "localhost";
const string postgresPort = "5432";
const string postgresUser = "postgres";
const string postgresPw = "Password!123";
const string postgresDB = "Region";

const string DELETE_REGION = "DELETE FROM inspection_region";
const string DELETE_GROUP = "DELETE FROM inspection_group";

const string INSERT_REGION = "INSERT INTO inspection_region (id, coord_x, coord_y, category, group_id) VALUES (";
const string INSERT_REGION1 = "INSERT INTO inspection_region (id, coord_x, coord_y, category, group_id) VALUES ($1, $2, $3, $4, $5);";
const string INSERT_GROUP = "INSERT INTO inspection_group (id) VALUES (";
const string END_INSERT = ");";

struct Config {
    string dataDirectory;
    string pointPath;
    string categoriesPath;
    string groupsPath;
};

struct Region {
    double x;
    double y;
    int category;
    int group;
};

bool isFileExist(string& filename)
{
    ifstream file(filename);
    return file.good();
}

void InsertToDB(vector<Region>& regionList, vector<int>& groupList)
{
    const string connStr = "host=" + postgresHost + " port=" + postgresPort +
        " dbname=" + postgresDB + " user=" + postgresUser + " password=" + postgresPw;
    
    try
    {
        pqxx::connection conn(connStr);

        if (!conn.is_open())
        {
            cout << "Connection failed." << endl;
            return;
        }
        cout << "Connection success." << endl;
        
        pqxx::work work(conn);
        work.exec(DELETE_REGION);
        work.exec(DELETE_GROUP);

        // insert data
        set<int> uniqueGroups(groupList.begin(), groupList.end());
        for (int groupId : uniqueGroups)
        {
            string groupSQL = INSERT_GROUP + to_string(groupId) + END_INSERT;
            work.exec(groupSQL);
        }
        
        for (int i = 0; i < regionList.size(); i++)
        {
            
            string regionSQL = INSERT_REGION + to_string(i) + ","
                + to_string(regionList[i].x) + ","
                + to_string(regionList[i].y) + ","
                + to_string(regionList[i].category) + ","
                + to_string(regionList[i].group) + END_INSERT;
            work.exec(regionSQL);
            
            // the upper code isn't ideal, I tried the code below, but not working
            // it hangs there, even I rebuild the libpqxx. that's why I keep the 
            // code above, I may fix this when I have time.
            /*
            pqxx::params params;
            params.append(i);
            params.append(regionList[i].x);
            params.append(regionList[i].y);
            params.append(regionList[i].category);
            params.append(regionList[i].group);
            work.exec(INSERT_REGION1, params);
            */
        }
        
        work.commit();
        cout << "Data Loaded." << endl;
    }
    catch (const pqxx::sql_error& e)
    {
        cerr << "SQL error: " << e.what() << endl;
        cerr << "Query: " << e.query() << endl;
    }
    catch (const exception& e)
    {
        cerr << "Error: " << e.what() << endl;
    }
    catch (...)
    {
        cerr << "Error: Unknown error." << endl;
    }
    
}

void Parser(Config& config)
{
    vector<Region> regionList;
    vector<int> groupList;
    bool hasError = false;
    ifstream pointFile;
    ifstream categoryFile;
    ifstream groupFile;

    try
    {
        pointFile.open(config.pointPath);
        if (!pointFile)
        {
            cerr << "Error: could not open the point file.\n";
            return;
        }

        categoryFile.open(config.categoriesPath);
        if (!categoryFile)
        {
            cerr << "Error: could not open the category file.\n";
            return;
        }

        groupFile.open(config.groupsPath);
        if (!groupFile)
        {
            cerr << "Error: could not open the group file.\n";
            return;
        }

        // files are ready
        double x;
        double y;
        double category;
        double group;
        
        while (pointFile >> x >> y &&
            categoryFile >> category &&
            groupFile >> group)
        {
            regionList.push_back({ x,y,static_cast<int>(category), static_cast<int>(group) });
            groupList.push_back(static_cast<int>(group));
        }
    }
    catch (const runtime_error& e)
    {
        hasError = true;
        cerr << "Runtim error: " << e.what() << endl;
    }
    catch (const exception& e)
    {
        hasError = true;
        cerr << "Exception: " << e.what() << endl;
    }
    catch (...)
    {
        hasError = true;
        cerr << "Error: Unknown error." << endl;
    }

    if (pointFile.is_open())
    {
        pointFile.close();
    }
    if (categoryFile.is_open())
    {
        categoryFile.close();
    }
    if (groupFile.is_open())
    {
        groupFile.close();
    }

    if (hasError)
    {
        return;
    }

    InsertToDB(regionList, groupList);
}

int main(int numArgs, char* args[])
{
    Config config;

    for (int i = 1; i < numArgs; i++)
    {
        string arg = args[i];
        cout << arg << endl;
        if (arg == DATA_DIRECTORY && i + 1 < numArgs)
        {
            config.dataDirectory = args[++i];
            cout << config.dataDirectory << endl;
            if (config.dataDirectory.back() != '\\')
            {
                config.dataDirectory.append("\\");
            }

            config.pointPath = config.dataDirectory + POINTS_FILE;
            config.categoriesPath = config.dataDirectory + CATEGORIES_FILE;
            config.groupsPath = config.dataDirectory + GROUPS_FILE;
            if (!isFileExist(config.pointPath))
            {
                cerr << "points.txt not exist." << endl;
            }
            else if (!isFileExist(config.categoriesPath))
            {
                cerr << "categories.txt not exist." << endl;
            }
            else if (!isFileExist(config.groupsPath))
            {
                cerr << "groups.txt not exist." << endl;
            }
            else
            {
                cout << "all files exist." << endl;
                i++; // jump to next arg
                Parser(config);
            }
        }
    }

    return 0;
}
