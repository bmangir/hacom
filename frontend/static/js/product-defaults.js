// Default image paths for each category
const categoryDefaultImages = {
    // Main categories
    'books': '/static/images/categories/book-default.png',
    'Books': '/static/images/categories/book-default.png',
    'clothing': '/static/images/categories/clothing-default.png',
    'Clothing': '/static/images/categories/clothing-default.png',
    'electronics': '/static/images/categories/electronics-default.png',
    'Electronics': '/static/images/categories/electronics-default.png',
    'home & kitchen': '/static/images/categories/home-default.png',
    'Home & Kitchen': '/static/images/categories/home-default.png',
    'sports & outdoors': '/static/images/categories/sports-default.png',
    'Sports & Outdoors': '/static/images/categories/sports-default.png',
    'beauty & personal care': '/static/images/categories/beauty-default.png',
    'Beauty & Personal Care': '/static/images/categories/beauty-default.png',
    'toys & games': '/static/images/categories/toys-default.png',
    'Toys & Games': '/static/images/categories/toys-default.png',
    'health & household': '/static/images/categories/health-default.png',
    'Health & Household': '/static/images/categories/health-default.png',
    'automotive': '/static/images/categories/automotive-default.png',
    'Automotive': '/static/images/categories/automotive-default.png',
    'pet supplies': '/static/images/categories/pet-default.png',
    'Pet Supplies': '/static/images/categories/pet-default.png',
    'office products': '/static/images/categories/office-default.png',
    'Office Products': '/static/images/categories/office-default.png',
    
    // Electronics subcategories
    'accessories': '/static/images/categories/accessories-default.jpg',
    'Accessories': '/static/images/categories/accessories-default.jpg',
    'audio': '/static/images/categories/audio-default.jpeg',
    'Audio': '/static/images/categories/audio-default.jpeg',
    'computers': '/static/images/categories/computers-default.jpeg',
    'Computers': '/static/images/categories/computers-default.jpeg',
    'mobile devices': '/static/images/categories/mobile-devices-default.png',
    'Mobile Devices': '/static/images/categories/mobile-devices-default.png',
    'photography': '/static/images/categories/photography-default.jpeg',
    'Photography': '/static/images/categories/photography-default.jpeg',
    'wearables': '/static/images/categories/wearables-default.jpeg',
    'Wearables': '/static/images/categories/wearables-default.jpeg',
    
    // Default fallback
    'default': '/static/images/categories/default-product.png'
};

function getDefaultImageForCategory(category) {
    if (!category) return categoryDefaultImages.default;
    // Try exact match first
    if (categoryDefaultImages[category]) {
        return categoryDefaultImages[category];
    }
    // Try lowercase match
    const categoryLower = category.toLowerCase();
    return categoryDefaultImages[categoryLower] || categoryDefaultImages.default;
}

// Function to handle image errors and replace with category-specific default
function handleProductImageError(img, category) {
    img.onerror = null; // Prevent infinite loop
    img.src = getDefaultImageForCategory(category);
} 