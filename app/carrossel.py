def get_carousel():
    carousel_html = """
    <style>
        .carousel-container {
            width: 67%;
            overflow: hidden;
            position: relative;
            background-color: #000;
            padding: 10px 0;
            margin: 0 auto;
        }
        .carousel-slide {
            display: flex;
            width: 200%;
            animation: slide 10s linear infinite;
        }
        .carousel-slide img {
            width: 75px;
            height: auto;
            margin: 0 15px;
        }
        @keyframes slide {
            0% { transform: translateX(0%); }
            50% { transform: translateX(-30%); }
            100% { transform: translateX(0%); }
        }
    </style>

    <div class="carousel-container">
        <div class="carousel-slide">
            <img src="https://upload.wikimedia.org/wikipedia/commons/6/69/IMDB_Logo_2016.svg"> <!-- IMDB -->
            <img src="https://upload.wikimedia.org/wikipedia/commons/5/5b/Rotten_Tomatoes.svg"> <!-- Rotten Tomatoes -->
            <img src="https://upload.wikimedia.org/wikipedia/commons/8/89/Tmdb.new.logo.svg"> <!-- The Movie Database -->
            <img src="https://ui.fstatic.com/static/images/header-filmow-logo.png"> <!-- Filmow -->
            <img src="https://assets.adorocinema.com/skin/img/adorocinema/logo-main.6419f574.svg"> <!-- AdoroCinema -->
            <img src="https://upload.wikimedia.org/wikipedia/commons/b/b7/Letterboxd-Logo-H-Pos-RGB.svg"> <!-- Letterboxd -->
            <img src="https://trakt.tv/assets/logos/logomark.square.gradient-b644b16c38ff775861b4b1f58c1230f6a097a2466ab33ae00445a505c33fcb91.svg"> <!-- Trakt -->
        </div>
    </div>
    """
    return carousel_html
