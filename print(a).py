from elasticsearch import Elasticsearch, helpers
import pandas as pd
es = Elasticsearch("http://localhost:9200")
employee_data = pd.read_csv('employee_survey_data.csv')
def createCollection(p_collection_name):
    es.indices.create(index=p_collection_name, ignore=400)
def indexData(p_collection_name, p_exclude_column):
    data = employee_data.drop(columns=[p_exclude_column]).to_dict(orient='records')
    helpers.bulk(es, [{"_index": p_collection_name, "_source": emp} for emp in data])
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {"query": {"match": {p_column_name: p_column_value}}}
    return es.search(index=p_collection_name, body=query)
def getEmpCount(p_collection_name):
    return es.count(index=p_collection_name)["count"]
def delEmpById(p_collection_name, p_employee_id):
    es.delete(index=p_collection_name, id=p_employee_id)
def getDepFacet(p_collection_name):
    query = {
        "aggs": {
            "departments": {
                "terms": {"field": "Department.keyword"}
            }
        },
        "size": 0
    }
    return es.search(index=p_collection_name, body=query)
v_nameCollection = 'Hash_Riyas'
v_phoneCollection = 'Hash_2345'
createCollection(v_nameCollection)
createCollection(v_phoneCollection)
print("Employee Count:", getEmpCount(v_nameCollection))
indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')
delEmpById(v_nameCollection, 'E02003')
print("Employee Count after deletion:", getEmpCount(v_nameCollection))
print("Search by Department (IT):", searchByColumn(v_nameCollection, 'Department', 'IT'))
print("Search by Gender (Male):", searchByColumn(v_nameCollection, 'Gender', 'Male'))
print("Search by Department (Phone collection, IT):", searchByColumn(v_phoneCollection, 'Department', 'IT'))
print("Department Facet (Name collection):", getDepFacet(v_nameCollection))
print("Department Facet (Phone collection):", getDepFacet(v_phoneCollection))