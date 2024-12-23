using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orderly.Consumer.Entities
{
    public class OrderEvent
    {
        public Guid Id { get; set; }
        public string Operation { get; set; }
        public Order Order { get; set; }
    }
}
