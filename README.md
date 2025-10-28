# Queries

1. Patients with more than 1 study

```json
  [
    {
      "$lookup": {
        "from": "fact_study",
        "localField": "patient_id",
        "foreignField": "patient_id",
        "as": "studies"
      }
    },
    {
      "$unwind": "$studies"
    },
    {
      "$lookup": {
        "from": "dim_protocol",
        "localField": "studies.protocol_id",
        "foreignField": "protocol_id",
        "as": "protocol"
      }
    },
    {
      "$unwind": "$protocol"
    },
    {
      "$lookup": {
        "from": "dim_study_date",
        "localField": "studies.study_date_id",
        "foreignField": "study_date_id",
        "as": "study_dates"
      }
    },
    {
      "$unwind": "$study_dates"
    },
    {
      "$group": {
        "_id": "$patient_id",
        "studies_count": { "$sum": 1 },
        "studies": {
          "$push": {
            "body_part_examined": "$protocol.body_part_examined",
            "study_date": "$study_dates.year",
            "study_id": "$studies.study_id"
          }
        }
      }
    },
    {
      "$match": {
        "studies_count": { "$gt": 1 }
      }
    },
    {
      "$project": {
        "patient_age": 1,
        "studies_count": 1,
        "studies": 1
      }
    }
  ]
```
