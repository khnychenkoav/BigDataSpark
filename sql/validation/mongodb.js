const dbName = "reports";
const database = db.getSiblingDB(dbName);
[
  "sales_by_product",
  "sales_by_customer",
  "sales_by_time",
  "sales_by_store",
  "sales_by_supplier",
  "product_quality"
].forEach((collectionName) => {
  print(`${collectionName}: ${database[collectionName].countDocuments()}`);
});
