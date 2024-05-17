provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "emil_coinbase" {
  bucket = "emil-coinbase-bucket"  
  
}
