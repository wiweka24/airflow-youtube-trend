<!-- markdownlint-configure-file {
  "MD013": {
    "code_blocks": false,
    "tables": false
  },
  "MD033": false,
  "MD041": false
} -->

<div align="center">

# airflow youtube trend

this is airflow dag to periodically fetch data from youtube api, created for Rekayasa Data class project.<br>
Check the model made using the data [here][model]

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)

[Preview](#preview) •
[Author](#author) •
[Prerequisites](#prerequisites) •
[Getting Started](#getting-started) •
[Support File](#support-file)

</div>

## Preview
![airflow-preview][airflow-img]

## Author
- [Wiweka Yoga Sadewa](https://github.com/wiweka24)
- [Jovian Joy Reynaldo](https://github.com/jovianjr)
- [Baihaqi Mustafa S A](https://github.com/mustafabaihaqi)
- [Dinda Sabela Rahma W](https://github.com/dindasabela)
- [Wardatul Radhiyyah](https://github.com/WardatulRadhiyyah)

## Prerequisites
Download and install [docker][docker-doc] to your operating system.<br>
[Windows][docker-wind] or [Linux][docker-linux]

## Getting Started
Setting up project for local usage.
1. Clone or Download this repository
    ```shell
    https://github.com/wiweka24/airflow-youtube-trend.git
    ```
    if using SSH
    ```shell
    git@github.com:wiweka24/airflow-youtube-trend.git
    ```
2. Start the containers:
    ```shell
    $ docker compose up
    ```
3. Airflow will run in localhost, go to:
   ```shell
   https://localhost:5432
   ```
## Support File
- [Model][model]
- [Video Presentasi][video-presentasi]
- [File Presentasi][file-presentasi]
    
[airflow-img]: https://user-images.githubusercontent.com/70740913/210065095-904cbeef-708b-4bb2-b120-7a8f604a5812.png
[docker-doc]: https://docs.docker.com/desktop/
[docker-wind]: https://docs.docker.com/desktop/install/windows-install/
[docker-linux]: https://docs.docker.com/desktop/install/linux-install/
[model]: https://www.kaggle.com/code/jovianreynaldo/youtube-trend-classification
[video-presentasi]: https://drive.google.com/file/d/1HJT6OlJaHiGwElu-WDZTR90RlBW0QSJb/view
[file-presentasi]: https://www.canva.com/design/DAFTIGAdLic/vf9dB_-u_n1wKH41dvw9EQ/edit
