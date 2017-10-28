package Retail

/**
  * Created by varun on 2/8/17.
  */
case class Department(
                       departmentId: Int,
                       departmentName: String
                     )

case class Category(
                     categoryId: Int,
                     categoryDepartmentId: Int,
                     categoryName: String
                   )


case class Product(
                    productId: Int,
                    productCategoryId: Int,
                    productName: String,
                    productDescription: String,
                    productPrice: Float,
                    productImage: String
                  )

case class Customer(
                     customerId: Int,
                     customerFname: String,
                     customerLname: String,
                     customerEmail: String,
                     customerPassword: String,
                     customerStreet: String,
                     customerCity: String,
                     customerState: String,
                     customerZipcode: String
                   )

case class Orders(
                  orderId: Int,
                  orderDate: String,
                  orderCustomerId: Int,
                  orderStatus: String
                )

case class OrderItems(
                      orderItemId: Int,
                      orderItemOrderId: Int,
                      orderItemProductId: Int,
                      orderItemQuantity: Int,
                      orderItemSubtotal: Float,
                      orderItemProductPrice: Float
                    )
/*object retail {
  def main(args: Array[String]) = {
    val oi = new OrderItems(1, 1, 1, 2,100, 50)
    println(oi)
    val ordItem = new OrderItems(2, 1, 3, 3.0, 50.0)
    println(ordItem)
  }
}*/

