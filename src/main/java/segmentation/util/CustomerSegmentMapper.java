package segmentation.util;

import segmentation.avro.CustomerSegment;
import segmentation.model.CustomerStats;

public class CustomerSegmentMapper {

    public static CustomerSegment fromStats(String customerId, CustomerStats stats) {
        return CustomerSegment.newBuilder()
                .setCustomerId(customerId)
                .setTotalSpent(stats.getTotalSpent())
                .setOrderCount(stats.getOrderCount())
                .setAvgOrderValue(stats.getAvgOrderValue())
                .setCategoriesPurchased(stats.getCategoriesCount())
                .setCustomerSegment(stats.computeSegment())
                .setPurchaseBehavior(stats.computePurchaseBehavior())
                .build();
    }
}