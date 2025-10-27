// ExtendedOperators.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <algorithm>
#include <fstream>
#include <iostream>
#include <vector>

//#include <pqxx/pqxx>
#include <libpq-fe.h>
#include <cstdlib>
#include "json.hpp"

using namespace std;
using json = nlohmann::json;


const string QUERY_COMMAND = "--query";

const string postgresHost = "localhost";
const string postgresPort = "5432";
const string postgresUser = "postgres";
const string postgresPw = "Password!123";
const string postgresDB = "Region";

const string OPR_AND = "operator_and";
const string OPR_OR = "operator_or";
const string OPR_CROP = "operator_crop";

struct Region {
    int id;
    double x;
    double y;
    int category;
    int group;
};

struct Rect
{
    double xmin;
    double xmax;
    double ymin;
    double ymax;

    bool isContain(const Region& region)
    {
        return
            region.x >= xmin &&
            region.x <= xmax &&
            region.y >= ymin &&
            region.y <= ymax;
    }
};

struct QueryParams
{
    Rect validRegion;
    Rect cropRegion;
    int category;
    vector<int> oneOfGroups;
    bool proper;
};

vector<Region> QueryDB(QueryParams& params)
{
    cout << "Query DB." << endl;
    vector<Region> resultList;

    const string connStr = "host=" + postgresHost + " port=" + postgresPort +
        " dbname=" + postgresDB + " user=" + postgresUser + " password=" + postgresPw;

    try
    {
        // handle crop
        string query = "SELECT * FROM inspection_region where coord_x BETWEEN ";
        query.append(to_string(params.cropRegion.xmin));
        query.append(" AND ");
        query.append(to_string(params.cropRegion.xmax));
        query.append(" AND coord_y BETWEEN ");
        query.append(to_string(params.cropRegion.ymin));
        query.append(" AND ");
        query.append(to_string(params.cropRegion.ymax));

        // handle category
        if (params.category != -1)
        {
            query.append(" AND category = ");
            query.append(to_string(params.category));
        }

        // handle groups
        if (params.oneOfGroups.size() > 0)
        {
            string groups = "(";
            bool first = true;
            for (int i : params.oneOfGroups)
            {
                if (!first)
                {
                    groups.append(",");
                }
                else
                {
                    first = false;
                }
                groups.append(to_string(i));
            }
            groups.append(")");
            query.append(" AND group_id in ");
            query.append(groups);
        }

        // handle proper
        if (params.proper)
        {
            query.append(" AND group_id IN (SELECT group_id FROM inspection_region GROUP BY group_id, coord_x, coord_y HAVING coord_x >= ");
            query.append(to_string(params.validRegion.xmin));
            query.append(" AND coord_x <= ");
            query.append(to_string(params.validRegion.xmax));
            query.append(" AND coord_y >= ");
            query.append(to_string(params.validRegion.ymin));
            query.append(" AND coord_y <= ");
            query.append(to_string(params.validRegion.ymax));
            query.append(")");
        }
        query.append(" ORDER BY coord_y, coord_x;");
        cout << "Query: " << query << endl;

        //pqxx::connection conn(connStr);
        PGconn* conn = PQconnectdb(connStr.c_str());
        //if (!conn.is_open())
        if (PQstatus(conn) != CONNECTION_OK)
        {
            cerr << "Connection failed." << endl;
            PQfinish(conn);
            return resultList;
        }
        cout << "Connection success." << endl;
        //pqxx::work work(conn);
        //pqxx::result res = work.exec(query);
        //work.commit();
        PGresult* res = PQexec(conn, query.c_str());

        if (PQresultStatus(res) != PGRES_TUPLES_OK)
        {
            cerr << "Query failed:" << PQerrorMessage(conn) << std::endl;
            PQclear(res);
            PQfinish(conn);
            return resultList;
        }

        int numRows = PQntuples(res);
        int numCols = PQnfields(res);

        for (int row = 0; row < numRows && numRows > 0; row++)
        {
            Region region;
            region.id = atoi(PQgetvalue(res, row, 0));
            region.group = atoi(PQgetvalue(res, row, 1));
            region.x = atof(PQgetvalue(res, row, 2));
            region.y = atof(PQgetvalue(res, row, 3));
            region.category = atoi(PQgetvalue(res, row, 4));
            resultList.push_back(region);
        }
        /*
        for (const auto& row : res)
        {
            Region reg;
            reg.id = row["id"].as<int>();
            reg.x = row["coord_x"].as<double>();
            reg.y = row["coord_y"].as<double>();
            reg.category = row["category"].as<int>();
            reg.group = row["group_id"].as<int>();

            resultList.push_back(reg);
        }
        //res.clear();
        PQclear(res);
        */
    }
    /*
    catch (const pqxx::sql_error& e)
    {
        cerr << "SQL error: " << e.what() << endl;
        cerr << "Query: " << e.query() << endl;
    }
    */
    catch (const exception& e)
    {
        cerr << "Error: " << e.what() << endl;
    }
    catch (...)
    {
        cerr << "Error: Unknown error." << endl;
    }

    cout << resultList.size() << " rows." << endl;
    return resultList;
}

bool isIdExist(int id, vector<Region>& list)
{
    for (int i = 0; i < list.size(); i++)
    {
        if (id == list[i].id)
            return true;
    }
    return false;
}

vector<Region> HandleQueryJson(json& tmpJson, Rect& validRegion)
{
    vector<Region> resultList;
    if (tmpJson.contains(OPR_AND) && tmpJson[OPR_AND].is_array())
    {
        cout << "opr and" << endl;
        vector<Region> list0 = HandleQueryJson(tmpJson[OPR_AND][0], validRegion);
        vector<Region> list1 = HandleQueryJson(tmpJson[OPR_AND][1], validRegion);

        for (int i = 0; i < list0.size(); i++)
        {
            if (isIdExist(list0[i].id, list1))
            {
                resultList.push_back(list0[i]);
            }
        }
    }
    else if (tmpJson.contains(OPR_OR))
    {
        cout << "opr or" << endl;
        vector<Region> list0 = HandleQueryJson(tmpJson[OPR_OR][0], validRegion);
        vector<Region> list1 = HandleQueryJson(tmpJson[OPR_OR][1], validRegion);
        resultList = list0;
        for (int i = 0; i < list1.size(); i++)
        {
            if (!isIdExist(list1[i].id, list0))
            {
                resultList.push_back(list1[i]);
            }
        }
    }
    else if (tmpJson.contains(OPR_CROP))
    {
        cout << "opr crop" << endl;
        QueryParams qParam;
        qParam.validRegion = validRegion;

        json cropJson = tmpJson[OPR_CROP];
        Rect cropRegion{
            cropJson["region"]["p_min"]["x"],
            cropJson["region"]["p_max"]["x"],
            cropJson["region"]["p_min"]["y"],
            cropJson["region"]["p_max"]["y"],
        };
        qParam.cropRegion = cropRegion;

        if (cropJson.contains("category"))
        {
            qParam.category = cropJson["category"];
        }
        else
        {
            qParam.category = -1;
        }

        if (cropJson.contains("one_of_groups") != NULL)
        {
            for (int group : cropJson["one_of_groups"])
            {
                qParam.oneOfGroups.push_back(group);
            }
        }

        qParam.proper = false;
        if (cropJson.contains("proper"))
        {
            qParam.proper = cropJson["proper"];
        }

        resultList = QueryDB(qParam);
    }
    else
    {
        cerr << "invalid operator.";
    }
    return resultList;
}

bool compareRegion(const Region& r1, const Region& r2)
{
    if (r1.y != r2.y)
        return r1.y < r2.y;
    return r1.x < r2.x;
}

void ProcessQueryFile(string& queryFilePath)
{
    ifstream queryFile;

    try
    {
        queryFile.open(queryFilePath);
        if (!queryFile)
        {
            cerr << "Error: could not open the query file: "
                << queryFilePath << endl;
            return;
        }

        json queryJson = json::parse(queryFile);
        Rect validRegion{
            queryJson["valid_region"]["p_min"]["x"],
            queryJson["valid_region"]["p_max"]["x"],
            queryJson["valid_region"]["p_min"]["y"],
            queryJson["valid_region"]["p_max"]["y"],
        };

        if (!queryJson.contains("query"))
        {
            cerr << "Error: Invalid json file, missing query.";
        }
        
        json tmpJson = queryJson["query"];
        vector<Region> results = HandleQueryJson(tmpJson, validRegion);

        // sorting
        sort(results.begin(), results.end(), compareRegion);
        cout << "-----------------------------------------------------" << endl;
        for (Region r : results)
        {
            cout << "id:" << r.id << " x:" << r.x << " y:" << r.y << " category:" << r.category << " group:" << r.group << endl;
        }
        cout << "Num of rows: " << results.size() << endl;
    }
    catch (const runtime_error& e)
    {
        cerr << "Runtim error: " << e.what() << endl;
    }
    catch (const exception& e)
    {
        cerr << "Exception: " << e.what() << endl;
    }
    catch (...)
    {
        cerr << "Error: Unknown error." << endl;
    }
}


int main(int numArgs, char* args[])
{
    for (int i = 1; i < numArgs; i++)
    {
        string arg = args[i];
        cout << arg << endl;
        int pos = arg.find('=');
        cout << pos << endl;
        if (pos == string::npos)
        {
            cerr << "Invalid argument: " << arg;
            return 0;
        }

        string command = arg.substr(0, pos);
        cout << "command=" << command << endl;

        string filePath = arg.substr(pos + 1, arg.length() - (pos + 1));
        cout << "filePath=" << filePath << endl;

        if (command._Equal(QUERY_COMMAND))
        {
            ProcessQueryFile(filePath);
        }
    }
    return 0;
}