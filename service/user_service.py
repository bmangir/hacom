import pickle

from flask import request, jsonify, render_template, redirect, url_for, session, Flask

from controller import login_required


app = Flask(__name__)

@app.route("/")
def index():
    return "hello world"


def register():
    pass


@app.route('/forgot-password', methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        try:

            return jsonify({"message": "Password reset link has been sent to your email."})
        except Exception as e:
            print(e)

    return render_template('forgot_password.html')


@app.route('/reset-password/<token>', methods=["GET", "POST"])
def reset_password(token):
    if request.method == "POST":
        try:
            print("a")
        except Exception as e:
            print(e)

    return render_template('reset_password.html', token=token)


@app.route("/users/del", methods=["GET", "DEL"])
@login_required
def delete_account():
    try:
        print("a")
    except Exception as e:
        return redirect(url_for("user_blueprint.logout",
                                username=session['username']))


def load_model():
    with open('model_for_user_based/user_features.pkl', 'rb') as f:
        user_features = pickle.load(f)
    with open('model_for_user_based/similarity_df.pkl', 'rb') as f:
        similarity_df = pickle.load(f)
    with open('model_for_user_based/scaler.pkl', 'rb') as f:
        scaler = pickle.load(f)

    with open('model_for_user_based/user_interactions.pkl', 'rb') as f:
        user_interactions = pickle.load(f)

    with open('model_for_user_based/user_product_map.pkl', 'rb') as f:
        user_product_map = pickle.load(f)

    return user_features, similarity_df, scaler, user_interactions, user_product_map


def recommend(id, top_n):
    user_features, cosine_similarity_df, scaler, user_interactions, user_product_map = load_model()

    # Get similar users
    similar_users = cosine_similarity_df[id].sort_values(ascending=False).iloc[1:top_n+1]
    similar_users = similar_users.to_dict()  # Convert to dictionary for JSON response

    recommendations = recommend_items(id, cosine_similarity_df, user_interactions, user_product_map, top_n)

    return jsonify({"similar_users": similar_users}, {'recommended_products': recommendations})


def find_similar_users(user_id, similarity_df, top_n=5):
    """Find top-n users similar to the given user_id."""
    if user_id not in similarity_df.index:
        raise ValueError(f"User {user_id} not found in the data.")
    similar_users = similarity_df[user_id].sort_values(ascending=False).iloc[1:top_n+1]  # Exclude self
    return similar_users


def recommend_items(user_id, similarity_df, user_interactions, user_product_map, top_n=5):
    """Recommend items to the given user based on similar users."""
    if user_id not in similarity_df.index:
        raise ValueError(f"User {user_id} not found in the data.")

    # Find top-N similar users
    similar_users = similarity_df[user_id].sort_values(ascending=False).iloc[1:top_n+1].index.tolist()

    # Get all products interacted with by similar users
    similar_users_products = set()
    for similar_user in similar_users:
        if similar_user in user_product_map:
            similar_users_products.update(user_product_map[similar_user])

    # Get products the target user has already interacted with
    user_interacted_products = user_interactions.get(user_id, set())

    # Recommend products the user hasn't interacted with yet
    recommendations = [product for product in similar_users_products if product not in user_interacted_products]

    return recommendations[:top_n]


if __name__ == '__main__':
    app.run(host="127.0.0.1", port=5000, debug=True)