use std::{str::FromStr, sync::Arc};
use actix_crud_api::{setup_db, DBSchema, Status};
use actix_web::{delete, get, post, put, web, App, HttpResponse, HttpServer, Responder, Result};
use heed::{types::*, Database, Env, EnvOpenOptions};
use std::env;

struct DbEnv {
    env: Arc<Env>
}

#[post("/create-record")]
async fn create_record(db_env: web::Data<DbEnv> ,data: web::Json<DBSchema>) -> Result<String> {
    let db_handles = setup_db(db_env.env.clone()).map_err(|e| {
        actix_web::error::ErrorInternalServerError(e)
    })?;
    let mut wtxn = db_env.env.write_txn().map_err(|e| {
        actix_web::error::ErrorInternalServerError(e)
    })?;

    let uuid = uuid::Uuid::new_v4().to_string();
    db_handles.main_db.put(&mut wtxn, &uuid, &data).map_err(|e| {
        actix_web::error::ErrorInternalServerError(e)
    })?;
    
    let county_uuid = format!("{}-{}", data.county, uuid);
    db_handles.county_index.put(&mut wtxn, &county_uuid, &uuid).map_err(|e| {
        actix_web::error::ErrorInternalServerError(e)
    })?;
    
    db_handles.opened_index.put(&mut wtxn, &data.opened, &uuid).map_err(|e| {
        actix_web::error::ErrorInternalServerError(e)
    })?;

    db_handles.last_updated.put(&mut wtxn, &data.last_updated, &uuid).map_err(|e| {
        actix_web::error::ErrorInternalServerError(e)
    })?;

    let county_status_uuid = format!("{}-{}", data.county_status, uuid);
    db_handles.county_status.put(&mut wtxn, &county_status_uuid, &uuid).map_err(|e| {
        actix_web::error::ErrorInternalServerError(e)
    })?;

    wtxn.commit().map_err(|e| {
        actix_web::error::ErrorInternalServerError(e)
    })?;

    Ok(format!("Successfully Loaded into DataBase {}", uuid))
}

#[get("/read-record-by-uuid/{key}")]
async fn read_record_by_uuid(db: web::Data<DbEnv>, path: web::Path<String>) -> impl Responder {
    let key = path.into_inner();
    let rtxn = db.env.read_txn().unwrap();

    let main_db: Database<Str, SerdeBincode<DBSchema>> = db
        .env
        .open_database(&rtxn, Some("main_db"))
        .unwrap()
        .unwrap();

    let record = {
        match main_db.get(&rtxn, &key)  {
            Ok(Some(record)) => Some(record),
            Ok(_) => {
                return HttpResponse::NotFound().body(format!("No Record found with the uuid: {}", key))
            }
            Err(e) => {
                eprintln!("Database error: {}", e);
                return HttpResponse::InternalServerError().body("Database Error")
            }
        }
    };

    if let Some(record) = record {
        return HttpResponse::Ok().body(format!(
            "permit_link: {}\npermit_number: {}\nclient: {}\nopened_date: {}\nlast_updated: {}\n status_updated: {}\n county: {}\n county_status: {}\n manual_status: {}\naddress: {}",
            record.permit_link, record.permit_number, record.client, record.opened, record.last_updated, record.status_updated, record.county, record.county_status, record.manual_status, record.address
        ))
    }

    HttpResponse::Ok().body("No record Found")
}

#[put("/update-county-status/{uuid}/{new_status}")]
async fn update_county_status(db_env: web::Data<DbEnv>, path: web::Path<(String, String)>) -> Result<String> {
    let db_handles = setup_db(db_env.env.clone()).map_err(|e| {
        actix_web::error::ErrorInternalServerError(e)
    })?;
    let mut wtxn = db_env.env.write_txn().map_err(|e| {
        actix_web::error::ErrorInternalServerError(e)
    })?;
    let (uuid, c_status) = path.into_inner();

    let main_record= db_handles.main_db.get(&wtxn, &uuid).map_err(|e| {
        actix_web::error::ErrorInternalServerError(e)
    })?;


    if let Some(mut data) = main_record {
        let last_county_status = data.county_status.to_string();
        data.county_status = Status::from_str(&c_status).unwrap();

        db_handles.main_db.put(&mut wtxn, &uuid, &data).map_err(|e| {
            actix_web::error::ErrorInternalServerError(e)
        })?;

        println!("hello");

        let county_status_uuid = format!("{}-{}", last_county_status, uuid);
        
        db_handles.county_status.delete(&mut wtxn, &county_status_uuid).map_err(|e| {
            actix_web::error::ErrorInternalServerError(e)
        })?;

        println!("Successfully deleted the old county status from county index");
        println!("Inserting the new record in the country index");

        let new_county_status_uuid = format!("{}-{}", c_status, uuid);
        db_handles.county_status.put(&mut wtxn, &new_county_status_uuid, &uuid).map_err(|e| {
            actix_web::error::ErrorInternalServerError(e)
        })?;

        wtxn.commit().map_err(|e| {
            actix_web::error::ErrorInternalServerError(e)
        })?;

        return Ok("Successfully Updated the Record".to_string())
    }

   Ok("Failed to insert the Record".to_string())
}

#[delete("/delete-record/{uuid}")]
async fn delete_record(db_env: web::Data<DbEnv>, path: web::Path<String>) -> Result<String> {
    let db_handles = setup_db(db_env.env.clone()).map_err(|e| {
        actix_web::error::ErrorInternalServerError(e)
    })?;
    let mut wtxn = db_env.env.write_txn().map_err(|e| {
        actix_web::error::ErrorInternalServerError(e)
    })?;
    let uuid = path.into_inner();

    let main_data = db_handles.main_db.get(&wtxn, &uuid).map_err(|e| {
        actix_web::error::ErrorInternalServerError(e)
    })?;

    if let Some(record) = main_data {
        let county_uuid = format!("{}-{}", record.county, uuid);
        let update_county_status_uuid = format!("{}-{}", record.county_status, uuid);

        db_handles.county_index.delete(&mut wtxn, &county_uuid).map_err(|e| {
            actix_web::error::ErrorInternalServerError(e)
        })?;
        db_handles.county_status.delete(&mut wtxn, &update_county_status_uuid).map_err(|e| {
            actix_web::error::ErrorInternalServerError(e)
        })?;
        db_handles.opened_index.delete(&mut wtxn, &record.opened).map_err(|e| {
            actix_web::error::ErrorInternalServerError(e)
        })?;
        db_handles.last_updated.delete(&mut wtxn, &record.last_updated).map_err(|e| {
            actix_web::error::ErrorInternalServerError(e)
        })?;

        db_handles.main_db.delete(&mut wtxn, &uuid).map_err(|e| {
            actix_web::error::ErrorInternalServerError(e)
        })?;

        wtxn.commit().map_err(|e| {
            actix_web::error::ErrorInternalServerError(e)
        })?;

        return Ok("Successfull deleted the record".to_string());
    } else {
        return Ok("Couldn't Delete the record".to_string())
    }

}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let env = match unsafe {
        EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024)
            .max_dbs(1000)
            .open("database")
    } {
        Ok(env) => Arc::new(env),
        Err(e) => {
            println!("Failed to load the env: {}", e);
            std::process::exit(1);
        }
    };

    println!("Environment Opened Successfully");

    let db_state = web::Data::new(DbEnv {
        env
    });
    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("0.0.0.0:{}", port);


    HttpServer::new(move || {
        App::new()
            .app_data(db_state.clone())
            .service(create_record)
            .service(read_record_by_uuid)
            .service(update_county_status)
            .service(delete_record)
        
    })
    .bind(addr)?
    .run()
    .await
}
