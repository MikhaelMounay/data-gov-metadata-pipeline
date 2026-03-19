from __future__ import annotations

import os

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for
from sqlalchemy.exc import SQLAlchemyError

from data_gov_datasets_explorer.models import ProjectCategoryEnum
from data_gov_datasets_explorer.webapp.services import (
    add_dataset_usage,
    create_project,
    dataset_count,
    datasets_by_format,
    datasets_by_org_type,
    datasets_by_tag,
    fetch_user_usage,
    register_user as register_user_service,
    search_datasets,
    top_5_datasets_by_users,
    top_5_organizations,
    top_10_tags_by_project_type,
    totals_grouped,
    usage_distribution_by_project_type,
)


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["SECRET_KEY"] = os.getenv(
        "FLASK_SECRET_KEY", "dev-key-change-me-before-production"
    )

    @app.get("/")
    def index() -> str:
        return redirect(url_for("dashboard"))

    @app.get("/dashboard")
    def dashboard() -> str:
        totals = totals_grouped()
        context = {
            "page_title": "Dashboard Overview",
            "page_eyebrow": "Executive View",
            "active_nav": "dashboard",
            "kpis": {
                "datasets": dataset_count(),
                "organizations": len(totals["organization"]),
                "topics": len(totals["topic"]),
                "formats": len(totals["format"]),
            },
            "top_organizations": top_5_organizations(),
            "top_datasets_by_users": top_5_datasets_by_users(),
            "usage_distribution": usage_distribution_by_project_type(),
        }
        return render_template("dashboard.html", **context)

    @app.get("/actions/users")
    def users_action_page() -> str:
        return render_template(
            "actions_users.html",
            page_title="Register User",
            page_eyebrow="Actions",
            active_nav="actions-users",
        )

    @app.get("/actions/usage")
    def usage_action_page() -> str:
        return render_template(
            "actions_usage.html",
            page_title="Add Dataset Usage",
            page_eyebrow="Actions",
            active_nav="actions-usage",
        )

    @app.get("/actions/projects")
    def projects_action_page() -> str:
        return render_template(
            "actions_projects.html",
            page_title="Create Project",
            page_eyebrow="Actions",
            active_nav="actions-projects",
            project_categories=[c.value for c in ProjectCategoryEnum],
        )

    @app.get("/api/datasets/search")
    def search_datasets_api():
        query = request.args.get("q", "").strip()
        results = search_datasets(query=query)
        return jsonify(results)

    @app.get("/explore/user")
    def user_explorer() -> str:
        usage_email = request.args.get("usage_email", "").strip()
        user_usage = fetch_user_usage(usage_email) if usage_email else []

        return render_template(
            "explore_user.html",
            page_title="User Usage Explorer",
            page_eyebrow="User Related",
            active_nav="explore-user",
            filters={"usage_email": usage_email},
            user_usage=user_usage,
        )

    @app.get("/explore/datasets")
    def dataset_explorer() -> str:
        org_type = request.args.get("org_type", "").strip()
        dataset_format = request.args.get("dataset_format", "").strip()
        dataset_tag = request.args.get("dataset_tag", "").strip()

        by_org_type = datasets_by_org_type(org_type) if org_type else []
        by_format = datasets_by_format(dataset_format) if dataset_format else []
        by_tag = datasets_by_tag(dataset_tag) if dataset_tag else []

        return render_template(
            "explore_datasets.html",
            page_title="Dataset Explorer",
            page_eyebrow="Catalog Discovery",
            active_nav="explore-datasets",
            filters={
                "org_type": org_type,
                "dataset_format": dataset_format,
                "dataset_tag": dataset_tag,
            },
            datasets_by_org_type=by_org_type,
            datasets_by_format=by_format,
            datasets_by_tag=by_tag,
        )

    @app.get("/analytics/organizations")
    def organizations_analytics() -> str:
        totals = totals_grouped()
        return render_template(
            "analytics_organizations.html",
            page_title="Organization Analytics",
            page_eyebrow="Organization Related",
            active_nav="analytics-organizations",
            top_organizations=top_5_organizations(),
            totals_grouped=totals,
        )

    @app.get("/analytics/projects")
    def project_analytics() -> str:
        return render_template(
            "analytics_projects.html",
            page_title="Project Analytics",
            page_eyebrow="Project Related",
            active_nav="analytics-projects",
            top_datasets_by_users=top_5_datasets_by_users(),
            usage_distribution=usage_distribution_by_project_type(),
        )

    @app.get("/analytics/tags")
    def tag_analytics() -> str:
        totals = totals_grouped()
        return render_template(
            "analytics_tags.html",
            page_title="Tag and Topic Analytics",
            page_eyebrow="Tag Related",
            active_nav="analytics-tags",
            top_tags_by_project_type=top_10_tags_by_project_type(),
            totals_grouped=totals,
        )

    @app.post("/register")
    def register_user() -> str:
        email = request.form.get("email", "").strip()
        username = request.form.get("username", "").strip()
        gender = request.form.get("gender", "").strip()
        birthdate = request.form.get("birthdate", "").strip()
        country = request.form.get("country", "").strip()

        if not email:
            flash("Email is required.", "error")
            return redirect(url_for("users_action_page"))

        try:
            flash(
                register_user_service(
                    email=email,
                    username=username,
                    gender=gender,
                    birthdate=birthdate,
                    country=country,
                ),
                "success",
            )
        except ValueError as exc:
            flash(str(exc), "error")
        except SQLAlchemyError as exc:
            flash(f"Database error while creating user: {exc}", "error")

        return redirect(url_for("users_action_page"))

    @app.post("/usage")
    def add_usage() -> str:
        email = request.form.get("app_user_email", "").strip()
        project_name = request.form.get("project_name", "").strip()
        dataset_id = request.form.get("dataset_id", "").strip()

        if not email or not project_name or not dataset_id:
            flash("User email, project name, and dataset id are required.", "error")
            return redirect(url_for("usage_action_page"))

        try:
            flash(
                add_dataset_usage(
                    email=email,
                    project_name=project_name,
                    dataset_id=dataset_id,
                ),
                "success",
            )
        except ValueError as exc:
            flash(str(exc), "error")
        except SQLAlchemyError as exc:
            flash(f"Database error while adding usage: {exc}", "error")

        return redirect(url_for("usage_action_page"))

    @app.post("/projects")
    def add_project() -> str:
        email = request.form.get("app_user_email", "").strip()
        project_name = request.form.get("project_name", "").strip()
        project_category = request.form.get("project_category", "").strip()

        if not email or not project_name:
            flash("User email and project name are required.", "error")
            return redirect(url_for("projects_action_page"))

        try:
            flash(
                create_project(
                    email=email,
                    project_name=project_name,
                    project_category_raw=project_category,
                ),
                "success",
            )
        except ValueError as exc:
            flash(str(exc), "error")
        except SQLAlchemyError as exc:
            flash(f"Database error while creating project: {exc}", "error")

        return redirect(url_for("projects_action_page"))

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.getenv("WEBAPP_PORT", "8000")), debug=True)
