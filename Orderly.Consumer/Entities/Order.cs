namespace Orderly.Consumer.Entities
{
    public class Order
    {
        public Guid Id { get; set; }
        public Guid CustomerId { get; set; }
        public DateTime OrderDate { get; set; }
        public decimal TotalAmount { get; set; }
        public OrderStatus Status { get; set; }
        public ICollection<OrderItem> OrderItems { get; set; } = new List<OrderItem>();
    }

    public enum OrderStatus
    {
        Pending,
        Processing,
        Completed,
        Cancelled
    }
}
