using System.Data.Common;
using System.Globalization;
using Northwind.Services.Repositories;

namespace Northwind.Services.Ado.Repositories
{
    public sealed class OrderRepository : IOrderRepository
    {
        private readonly DbProviderFactory dbFactory;
        private readonly string connectionString;

        public OrderRepository(DbProviderFactory dbFactory, string connectionString)
        {
            this.dbFactory = dbFactory;
            this.connectionString = connectionString;
        }

        public async Task<long> AddOrderAsync(Order order)
        {
            using var dbConnection = this.dbFactory.CreateConnection() ?? throw new InvalidOperationException("Failed");
            dbConnection.ConnectionString = this.connectionString;
            await dbConnection.OpenAsync();
            using var dbTransaction = dbConnection.BeginTransaction();
            using DbCommand command = dbConnection.CreateCommand();
            command.Connection = dbConnection;
            try
            {
                command.Transaction = dbTransaction;
                command.CommandText = $"INSERT INTO Orders (CustomerID, EmployeeID, OrderDate, RequiredDate, ShippedDate, ShipVia, Freight, ShipName, ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry) VALUES (@CustomerID, @EmployeeID, @OrderDate, @RequiredDate, @ShippedDate, @ShipVia, @Freight, @ShipName, @ShipAddress, @ShipCity, @ShipRegion, @ShipPostalCode, @ShipCountry); SELECT last_insert_rowid();";

                DbParameter customerId = command.CreateParameter();
                customerId.DbType = System.Data.DbType.String;
                customerId.Value = order.Customer.Code.Code;
                customerId.ParameterName = "@CustomerID";
                _ = command.Parameters.Add(customerId);

                DbParameter employeeId = command.CreateParameter();
                employeeId.DbType = System.Data.DbType.Int64;
                employeeId.Value = order.Employee.Id;
                employeeId.ParameterName = "@EmployeeID";
                _ = command.Parameters.Add(employeeId);

                DbParameter orderDate = command.CreateParameter();
                orderDate.DbType = System.Data.DbType.DateTime;
                orderDate.Value = order.OrderDate;
                orderDate.ParameterName = "@OrderDate";
                _ = command.Parameters.Add(orderDate);

                DbParameter requiredDate = command.CreateParameter();
                requiredDate.DbType = System.Data.DbType.DateTime;
                requiredDate.Value = order.RequiredDate;
                requiredDate.ParameterName = "@RequiredDate";
                _ = command.Parameters.Add(requiredDate);

                DbParameter shippedDate = command.CreateParameter();
                shippedDate.DbType = System.Data.DbType.DateTime;
                shippedDate.Value = order.ShippedDate;
                shippedDate.ParameterName = "@ShippedDate";
                _ = command.Parameters.Add(shippedDate);

                DbParameter shipVia = command.CreateParameter();
                shipVia.DbType = System.Data.DbType.Int64;
                shipVia.Value = order.Shipper.Id;
                shipVia.ParameterName = "@ShipVia";
                _ = command.Parameters.Add(shipVia);

                DbParameter freight = command.CreateParameter();
                freight.DbType = System.Data.DbType.Double;
                freight.Value = order.Freight;
                freight.ParameterName = "@Freight";
                _ = command.Parameters.Add(freight);

                DbParameter shipName = command.CreateParameter();
                shipName.DbType = System.Data.DbType.String;
                shipName.Value = order.ShipName;
                shipName.ParameterName = "@ShipName";
                _ = command.Parameters.Add(shipName);

                DbParameter shipAddress = command.CreateParameter();
                shipAddress.DbType = System.Data.DbType.String;
                shipAddress.Value = order.ShippingAddress.Address;
                shipAddress.ParameterName = "@ShipAddress";
                _ = command.Parameters.Add(shipAddress);

                DbParameter shipCity = command.CreateParameter();
                shipCity.DbType = System.Data.DbType.String;
                shipCity.Value = order.ShippingAddress.City;
                shipCity.ParameterName = "@ShipCity";
                _ = command.Parameters.Add(shipCity);

                DbParameter shipRegion = command.CreateParameter();
                shipRegion.DbType = System.Data.DbType.String;
                shipRegion.Value = order.ShippingAddress?.Region ?? string.Empty;
                shipRegion.ParameterName = "@ShipRegion";
                _ = command.Parameters.Add(shipRegion);

                DbParameter shipPostalCode = command.CreateParameter();
                shipPostalCode.DbType = System.Data.DbType.String;
                shipPostalCode.Value = order.ShippingAddress?.PostalCode;
                shipPostalCode.ParameterName = "@ShipPostalCode";
                _ = command.Parameters.Add(shipPostalCode);

                DbParameter shipCountry = command.CreateParameter();
                shipCountry.DbType = System.Data.DbType.String;
                shipCountry.Value = order.ShippingAddress?.Country;
                shipCountry.ParameterName = "@ShipCountry";
                _ = command.Parameters.Add(shipCountry);

                CultureInfo culture = CultureInfo.InvariantCulture;
                var orderRowId = Convert.ToInt64(await command.ExecuteScalarAsync(), culture);

                command.CommandText = $"DELETE FROM OrderDetails WHERE OrderID = @OrderID";
                command.Parameters.Clear();
                DbParameter orderIdParameter = command.CreateParameter();
                orderIdParameter.ParameterName = "@OrderID";
                orderIdParameter.DbType = System.Data.DbType.Int64;
                orderIdParameter.Value = orderRowId;
                _ = command.Parameters.Add(orderIdParameter);
                _ = await command.ExecuteNonQueryAsync();

                foreach (var orderDetail in order.OrderDetails)
                {
                    command.CommandText = $"INSERT INTO OrderDetails (OrderID, ProductID, UnitPrice, Quantity, Discount) VALUES (@OrderID, @ProductID, @UnitPrice, @Quantity, @Discount)";
                    command.Parameters.Clear();

                    DbParameter productID = command.CreateParameter();
                    productID.DbType = System.Data.DbType.Int64;
                    productID.Value = orderDetail.Product.Id;
                    productID.ParameterName = "@ProductID";
                    _ = command.Parameters.Add(productID);

                    DbParameter unitPrice = command.CreateParameter();
                    unitPrice.DbType = System.Data.DbType.Double;
                    unitPrice.Value = orderDetail.UnitPrice;
                    unitPrice.ParameterName = "@UnitPrice";
                    _ = command.Parameters.Add(unitPrice);

                    DbParameter quantity = command.CreateParameter();
                    quantity.DbType = System.Data.DbType.Int64;
                    quantity.Value = orderDetail.Quantity;
                    quantity.ParameterName = "@Quantity";
                    _ = command.Parameters.Add(quantity);

                    DbParameter discount = command.CreateParameter();
                    discount.DbType = System.Data.DbType.Double;
                    discount.Value = orderDetail.Discount;
                    discount.ParameterName = "@Discount";
                    _ = command.Parameters.Add(discount);

                    DbParameter orderIdDetail = command.CreateParameter();
                    orderIdDetail.DbType = System.Data.DbType.Int64;
                    orderIdDetail.Value = orderRowId;
                    orderIdDetail.ParameterName = "@OrderID";
                    _ = command.Parameters.Add(orderIdDetail);

                    _ = await command.ExecuteNonQueryAsync();
                }

                dbTransaction.Commit();
                return orderRowId;
            }
            catch (Exception ex)
            {
                dbTransaction.Rollback();
                throw new RepositoryException(ex.Message);
            }
        }

        public async Task<Order> GetOrderAsync(long orderId)
        {
            if (orderId <= 0)
            {
                throw new RepositoryException("orderId <= 0!");
            }

            Order order = new Order(orderId);
            using (var dbConnection = this.dbFactory.CreateConnection() ?? throw new InvalidOperationException("Failed"))
            {
                dbConnection.ConnectionString = this.connectionString;
                await dbConnection.OpenAsync();
                using DbCommand command = dbConnection.CreateCommand();
                command.Connection = dbConnection;
                command.CommandText = $"SELECT *, Customers.CompanyName AS Cust_CompanyName, Shippers.CompanyName AS Ship_CompanyName, Employees.Country AS Em_Country, OrderDetails.UnitPrice AS OD_UnitPrice FROM ORDERS INNER JOIN Shippers ON Orders.ShipVia=Shippers.ShipperID INNER JOIN Employees ON Orders.EmployeeID = Employees.EmployeeID INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID INNER JOIN OrderDetails ON Orders.OrderID=OrderDetails.OrderID INNER JOIN Products ON OrderDetails.ProductID=Products.ProductID INNER JOIN Suppliers ON Products.SupplierID=Suppliers.SupplierID INNER JOIN Categories ON Products.CategoryID=Categories.CategoryID WHERE Orders.OrderID={orderId} ";

                using DbDataReader reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    order.OrderDate = reader.GetDateTime(reader.GetOrdinal("OrderDate"));
                    order.RequiredDate = reader.GetDateTime(reader.GetOrdinal("RequiredDate"));
                    order.ShippedDate = reader.GetDateTime(reader.GetOrdinal("ShippedDate"));
                    order.Freight = reader.GetDouble(reader.GetOrdinal("Freight"));
                    order.ShipName = reader.GetString(reader.GetOrdinal("ShipName"));
                    order.Customer = new Customer(new CustomerCode(reader.GetString(reader.GetOrdinal("CustomerID"))))
                    {
                        CompanyName = reader.GetString(reader.GetOrdinal("Cust_CompanyName")),
                    };
                    order.Employee = new Employee(reader.GetInt64(reader.GetOrdinal("EmployeeID")))
                    {
                        FirstName = reader.GetString(reader.GetOrdinal("FirstName")),
                        LastName = reader.GetString(reader.GetOrdinal("LastName")),
                        Country = reader.GetString(reader.GetOrdinal("Em_Country")),
                    };
                    order.Shipper = new Shipper(reader.GetInt64(reader.GetOrdinal("ShipperID")))
                    {
                        CompanyName = reader.GetString(reader.GetOrdinal("Ship_CompanyName")),
                    };
                    order.ShippingAddress = new ShippingAddress(
                                reader.GetString(reader.GetOrdinal("ShipAddress")),
                                reader.GetString(reader.GetOrdinal("ShipCity")),
                                reader.IsDBNull(reader.GetOrdinal("ShipRegion")) ? null : reader.GetString(reader.GetOrdinal("ShipRegion")),
                                reader.GetString(reader.GetOrdinal("ShipPostalCode")),
                                reader.GetString(reader.GetOrdinal("ShipCountry")));

                    var orderDetails = new OrderDetail(order)
                    {
                        Product = new Product(reader.GetInt64(reader.GetOrdinal("ProductID")))
                        {
                            ProductName = reader.GetString(reader.GetOrdinal("ProductName")),
                            SupplierId = reader.GetInt64(reader.GetOrdinal("SupplierID")),
                            Supplier = reader.GetString(reader.GetOrdinal("CompanyName")),
                            CategoryId = reader.GetInt64(reader.GetOrdinal("CategoryID")),
                            Category = reader.GetString(reader.GetOrdinal("CategoryName")),
                        },
                        UnitPrice = reader.GetDouble(reader.GetOrdinal("OD_UnitPrice")),
                        Quantity = reader.GetInt64(reader.GetOrdinal("Quantity")),
                        Discount = reader.GetDouble(reader.GetOrdinal("Discount")),
                    };
                    order.OrderDetails.Add(orderDetails);
                }
            }

            return order;
        }

        public async Task<IList<Order>> GetOrdersAsync(int skip, int count)
        {
            if (count <= 0 || skip < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }

            List<Order> orders = new List<Order>();
            using (var connection = this.dbFactory.CreateConnection() ?? throw new InvalidOperationException("Failed"))
            {
                connection.ConnectionString = this.connectionString;
                await connection.OpenAsync();
                using DbCommand command = connection.CreateCommand();
                command.Connection = connection;
                command.CommandText = $"SELECT DISTINCT OrderID FROM Orders ORDER BY OrderID ASC LIMIT {skip},{count}";
                using DbDataReader reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    long orderId = reader.GetInt64(reader.GetOrdinal("OrderID"));
                    Order order = await this.GetOrderAsync(orderId);
                    orders.Add(order);
                }
            }

            return orders;
        }

        public async Task RemoveOrderAsync(long orderId)
        {
            using var connection = this.dbFactory.CreateConnection() ?? throw new InvalidOperationException("Failed");
            connection.ConnectionString = this.connectionString;
            await connection.OpenAsync();
            using DbTransaction dbTransaction = await connection.BeginTransactionAsync();
            using DbCommand command = connection.CreateCommand();
            try
            {
                command.Connection = connection;
                command.Transaction = dbTransaction;
                command.CommandText = $"DELETE FROM OrderDetails WHERE OrderID = {orderId}";
                _ = await command.ExecuteNonQueryAsync();
                command.CommandText = $"DELETE FROM Orders WHERE OrderID = {orderId}";
                _ = await command.ExecuteNonQueryAsync();
                dbTransaction.Commit();
            }
            catch (Exception ex)
            {
                dbTransaction.Rollback();
                throw new RepositoryException(ex.Message);
            }
        }

        public async Task UpdateOrderAsync(Order order)
        {
            using var connection = this.dbFactory.CreateConnection() ?? throw new InvalidOperationException("Failed");
            connection.ConnectionString = this.connectionString;
            await connection.OpenAsync();
            DbTransaction dbTransaction = await connection.BeginTransactionAsync();
            try
            {
                using DbCommand command = connection.CreateCommand();
                command.Connection = connection;
                command.CommandText = $"UPDATE Orders SET CustomerID = @CustomerID, EmployeeID = @EmployeeID, OrderDate = @OrderDate, RequiredDate = @RequiredDate, ShippedDate = @ShippedDate, ShipVia = @ShipVia, Freight = @Freight, ShipName = @ShipName, ShipAddress = @ShipAddress, ShipCity = @ShipCity, ShipRegion = @ShipRegion, ShipPostalCode = @ShipPostalCode, ShipCountry = @ShipCountry WHERE OrderID = @OrderID";

                DbParameter customerId = command.CreateParameter();
                customerId.DbType = System.Data.DbType.String;
                customerId.Value = order.Customer.Code.Code;
                customerId.ParameterName = "@CustomerID";
                _ = command.Parameters.Add(customerId);

                DbParameter employeeId = command.CreateParameter();
                employeeId.DbType = System.Data.DbType.Int64;
                employeeId.Value = order.Employee.Id;
                employeeId.ParameterName = "@EmployeeID";
                _ = command.Parameters.Add(employeeId);

                DbParameter orderDate = command.CreateParameter();
                orderDate.DbType = System.Data.DbType.DateTime;
                orderDate.Value = order.OrderDate;
                orderDate.ParameterName = "@OrderDate";
                _ = command.Parameters.Add(orderDate);

                DbParameter requiredDate = command.CreateParameter();
                requiredDate.DbType = System.Data.DbType.DateTime;
                requiredDate.Value = order.RequiredDate;
                requiredDate.ParameterName = "@RequiredDate";
                _ = command.Parameters.Add(requiredDate);

                DbParameter shippedDate = command.CreateParameter();
                shippedDate.DbType = System.Data.DbType.DateTime;
                shippedDate.Value = order.ShippedDate;
                shippedDate.ParameterName = "@ShippedDate";
                _ = command.Parameters.Add(shippedDate);

                DbParameter shipVia = command.CreateParameter();
                shipVia.DbType = System.Data.DbType.Int64;
                shipVia.Value = order.Shipper.Id;
                shipVia.ParameterName = "@ShipVia";
                _ = command.Parameters.Add(shipVia);

                DbParameter freight = command.CreateParameter();
                freight.DbType = System.Data.DbType.Double;
                freight.Value = order.Freight;
                freight.ParameterName = "@Freight";
                _ = command.Parameters.Add(freight);

                DbParameter shipName = command.CreateParameter();
                shipName.DbType = System.Data.DbType.String;
                shipName.Value = order.ShipName;
                shipName.ParameterName = "@ShipName";
                _ = command.Parameters.Add(shipName);

                DbParameter shipAddress = command.CreateParameter();
                shipAddress.DbType = System.Data.DbType.String;
                shipAddress.Value = order.ShippingAddress.Address;
                shipAddress.ParameterName = "@ShipAddress";
                _ = command.Parameters.Add(shipAddress);

                DbParameter shipCity = command.CreateParameter();
                shipCity.DbType = System.Data.DbType.String;
                shipCity.Value = order.ShippingAddress.City;
                shipCity.ParameterName = "@ShipCity";
                _ = command.Parameters.Add(shipCity);

                DbParameter shipRegion = command.CreateParameter();
                shipRegion.DbType = System.Data.DbType.String;
                shipRegion.Value = order.ShippingAddress.Region;
                shipRegion.ParameterName = "@ShipRegion";
                _ = command.Parameters.Add(shipRegion);

                DbParameter shipPostalCode = command.CreateParameter();
                shipPostalCode.DbType = System.Data.DbType.String;
                shipPostalCode.Value = order.ShippingAddress.PostalCode;
                shipPostalCode.ParameterName = "@ShipPostalCode";
                _ = command.Parameters.Add(shipPostalCode);

                DbParameter shipCountry = command.CreateParameter();
                shipCountry.DbType = System.Data.DbType.String;
                shipCountry.Value = order.ShippingAddress.Country;
                shipCountry.ParameterName = "@ShipCountry";
                _ = command.Parameters.Add(shipCountry);

                DbParameter orderId = command.CreateParameter();
                orderId.DbType = System.Data.DbType.Int64;
                orderId.Value = order.Id;
                orderId.ParameterName = "@OrderID";
                _ = command.Parameters.Add(orderId);

                _ = await command.ExecuteNonQueryAsync();

                command.CommandText = $"DELETE FROM OrderDetails WHERE OrderID = @OrderID";
                _ = await command.ExecuteNonQueryAsync();

                foreach (var orderDetail in order.OrderDetails)
                {
                    command.CommandText = $"INSERT INTO OrderDetails (OrderID, ProductID, UnitPrice, Quantity, Discount) VALUES (@OrderID, @ProductID, @UnitPrice, @Quantity, @Discount)";
                    command.Parameters.Clear();

                    DbParameter productID = command.CreateParameter();
                    productID.DbType = System.Data.DbType.Int64;
                    productID.Value = orderDetail.Product.Id;
                    productID.ParameterName = "@ProductID";
                    _ = command.Parameters.Add(productID);

                    DbParameter unitPrice = command.CreateParameter();
                    unitPrice.DbType = System.Data.DbType.Double;
                    unitPrice.Value = orderDetail.UnitPrice;
                    unitPrice.ParameterName = "@UnitPrice";
                    _ = command.Parameters.Add(unitPrice);

                    DbParameter quantity = command.CreateParameter();
                    quantity.DbType = System.Data.DbType.Int64;
                    quantity.Value = orderDetail.Quantity;
                    quantity.ParameterName = "@Quantity";
                    _ = command.Parameters.Add(quantity);

                    DbParameter discount = command.CreateParameter();
                    discount.DbType = System.Data.DbType.Double;
                    discount.Value = orderDetail.Discount;
                    discount.ParameterName = "@Discount";
                    _ = command.Parameters.Add(discount);

                    DbParameter orderIdDetail = command.CreateParameter();
                    orderIdDetail.DbType = System.Data.DbType.Int64;
                    orderIdDetail.Value = order.Id;
                    orderIdDetail.ParameterName = "@OrderID";
                    _ = command.Parameters.Add(orderIdDetail);

                    _ = await command.ExecuteNonQueryAsync();
                }

                dbTransaction.Commit();
            }
            catch (Exception ex)
            {
                dbTransaction.Rollback();
                throw new RepositoryException(ex.Message);
            }
        }
    }
}
