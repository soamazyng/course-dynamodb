const express = require("express");
const app = express();

const AWS = require("aws-sdk");

var bodyParser = require("body-parser");

app.use(bodyParser.json()); // to supoort json-encoded bodies
app.use(
  bodyParser.urlencoded({
    // to support URL-encoded bodies
    extended: true,
  })
);

// make all the files in 'public' available
// https://expressjs.com/en/starter/static-files.html
app.use(express.static("public"));

// https://expressjs.com/en/starter/basic-routing.html
app.get("/", (request, response) => {
  response.send("OK Express");
});

app.get("/listarTabelas", (request, response) => {
  AWS.config.update({ region: "us-east-1" });

  var dynamoDB = new AWS.DynamoDB();

  var params = {};

  dynamoDB.listTables(params, function (err, data) {
    if (err) {
      response.json(err);
      console.log(err);
    } else {
      response.json(data);
    }
  });
});

// Inclusao de Dados no DynamoDB
app.post("/inserir", (request, response) => {
  AWS.config.update({ region: "us-east-1" });

  var client = new AWS.DynamoDB.DocumentClient();

  var params = {
    TableName: "clientes1",
    Item: {
      email: request.body.email,
      nome: request.body.nome,
      data_nascimento: request.body.data_nascimento,
      idade: request.body.idade,
    },
  };

  client.put(params, function (err, data) {
    if (err) {
      response.json(err);
      console.log(err);
    } else {
      response.json(data);
    }
  });
});

// Inclusao de Dados no DynamoDB
app.put("/atualizar-salario/:emailId/:nomeId", (request, response) => {
  AWS.config.update({ region: "us-east-1" });

  var id_cliente = request.params.emailId;
  var nome_cliente = request.params.nomeId;

  var salario = request.body.salario;
  var idade = request.body.idade;

  var client = new AWS.DynamoDB.DocumentClient();

  var params = {
    TableName: "clientes",
    Key: {
      email: id_cliente,
      nome: nome_cliente,
    },
    UpdateExpression: "set #s = :y",
    ConditionExpression: "idade >= :x",
    ExpressionAttributeNames: {
      "#s": "salario",
    },
    ExpressionAttributeValues: {
      ":y": salario,
      ":x": idade,
    },
  };

  client.update(params, function (err, data) {
    if (err) {
      response.json(err);
      console.log(err);
    } else {
      response.json(data);
    }
  });
});

app.delete("/excluir/:emailId/:nomeId", (request, response) => {
  AWS.config.update({ region: "us-east-1" });

  var client = new AWS.DynamoDB.DocumentClient();

  var id_cliente = request.params.emailId;
  var nome_cliente = request.params.nomeId;

  var params = {
    TableName: "clientes",
    Key: {
      email: id_cliente,
      nome: nome_cliente,
    },
  };

  client.delete(params, function (err, data) {
    if (err) {
      response.json(err);
      console.log(err);
    } else {
      response.json(data);
    }
  });
});

app.get("/recuperar/:emailId/:nomeId", (request, response) => {
  AWS.config.update({ region: "us-east-1" });

  var client = new AWS.DynamoDB.DocumentClient();

  var id_cliente = request.params.emailId;
  var nome_cliente = request.params.nomeId;

  var params = {
    TableName: "clientes",
    Key: {
      email: id_cliente,
      nome: nome_cliente,
    },
    AttributesToGet: ["nome", "email"],
    ConsistentRead: false,
    ReturnConsumedCapacity: "TOTAL",
  };

  client.get(params, function (err, data) {
    if (err) {
      response.json(err);
      console.log(err);
    } else {
      response.json(data);
    }
  });
});

app.get("/buscar/", (request, response) => {
  AWS.config.update({ region: "us-east-1" });

  var client = new AWS.DynamoDB.DocumentClient();

  var params = {
    TableName: "clientes",
    FilterExpression: "idade >= :idade",
    ExpressionAttributeValues: { ":idade": 30 },
    ReturnConsumedCapacity: "TOTAL",
  };

  client.scan(params, function (err, data) {
    if (err) {
      response.json(err);
      console.log(err);
    } else {
      response.json(data);
    }
  });
});

app.get("/consultar/", (request, response) => {
  AWS.config.update({ region: "us-east-1" });

  var client = new AWS.DynamoDB.DocumentClient();

  var params = {
    TableName: "clientes",
    IndexName: "nome-email-index",
    // KeyConditionExpression: "nome = :hkey",
    KeyConditions: {
      nome: {
        ComparisonOperator: "CONTAINS",
        AttributeValueList: [{ S: "No One You Know" }],
      },
      email: {
        ComparisonOperator: "CONTAINS",
        AttributeValueList: [{ S: "No One You Know" }],
      },
    },
    // ExpressionAttributeValues: {
    //   ":hkey": "teste"
    // },
  };

  client.query(params, function (err, data) {
    if (err) {
      response.json(err);
      console.log(err);
    } else {
      response.json(data);
    }
  });
});

app.get("/ler-lote/:emailId/:nomeId/:cpf", (request, response) => {
  AWS.config.update({ region: "us-east-1" });

  var client = new AWS.DynamoDB.DocumentClient();

  var email_cliente = request.params.emailId;
  var nome_cliente = request.params.nomeId;
  var cpf_cliente = request.params.cpf;

  var params = {
    RequestItems: {
      clientes: {
        Keys: [
          {
            nome: nome_cliente,
            email: email_cliente,
          },
        ],
      },
      clientes1: {
        Keys: [
          {
            cpf: cpf_cliente,
            email: email_cliente,
          },
        ],
      },
    },
  };

  client.batchGet(params, function (err, data) {
    if (err) {
      response.json(err);
      console.log(err);
    } else {
      response.json(data);
    }
  });
});

app.post("/escrever-lote/", (request, response) => {
  AWS.config.update({ region: "us-east-1" });

  var client = new AWS.DynamoDB.DocumentClient();

  var params = {
    RequestItems: {
      clientes: [
        {
          PutRequest: {
            Item: {
              email: request.body.email,
              nome: request.body.nome,
              data_nascimento: request.body.data_nascimento,
              idade: request.body.idade,
            },
          },
        },
      ],
      clientes1: [
        {
          PutRequest: {
            Item: {
              cpf: request.body.cpf,
              email: request.body.email,
              nome: request.body.nome,
            },
          },
        },
      ],
    },
  };

  client.batchWrite(params, function (err, data) {
    if (err) {
      response.json(err);
      console.log(err);
    } else {
      response.json(data);
    }
  });
});

// listen for requests :)
const listener = app.listen(process.env.PORT, () => {
  console.log("Your app is listening on port " + listener.address().port);
});
